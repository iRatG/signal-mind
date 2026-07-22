"""Build a lightweight news-pressure snapshot from archive headlines.

This is intentionally a spike: collect source/date/title/url rows, then produce
a compact Markdown report that shows which topics dominate and which ones
accelerated in the last week versus the previous window.

Usage:
    python analytics/news_pressure_snapshot.py --days 30
    python analytics/news_pressure_snapshot.py --from 2026-06-22 --to 2026-07-21
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlunparse

import requests
import yaml


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "news_pressure"
DEFAULT_DB_PATH = OUT_DIR / "news_pressure.db"
DEFAULT_SOURCES_CONFIG = ROOT / "config" / "news_pressure_sources.yaml"

REQUEST_TIMEOUT = 35
REQUEST_SLEEP_SECONDS = 0.35
USER_AGENT = (
    "Mozilla/5.0 (compatible; SignalMindNewsPressureSnapshot/0.1; "
    "+https://github.com/iRatG/signal-mind)"
)

STOPWORDS = {
    "что", "как", "это", "или", "для", "при", "над", "под", "без", "после",
    "было", "будет", "более", "менее", "себя", "свои", "свой", "также",
    "из-за", "изза", "россии", "россия", "российский", "российские",
    "заявил", "сообщил", "рассказал", "назвал", "стало", "стали",
    "января", "февраля", "марта", "апреля", "мая", "июня", "июля",
    "августа", "сентября", "октября", "ноября", "декабря",
}

TOPICS = {
    "war_security": ["войн", "удар", "дрон", "беспилот", "ракет", "фронт", "оборон", "безопасн"],
    "politics_state": ["госдум", "правительств", "министр", "президент", "кремл", "закон", "выбор"],
    "economy_markets": ["эконом", "рынок", "бирж", "акци", "индекс", "инвест", "компан"],
    "money_rates": ["рубл", "доллар", "евро", "ставк", "цб", "банк", "кредит", "ипотек"],
    "energy_raw": ["нефт", "газ", "энерг", "топлив", "бензин", "брент", "угол"],
    "sanctions_trade": ["санкц", "пошлин", "экспорт", "импорт", "торгов", "огранич"],
    "tech_ai": ["искусственн", "нейросет", "технолог", "данн", "робот", "чип", "софт"],
    "society_life": ["школ", "врач", "медицин", "жиль", "семь", "дет", "город", "жител"],
}


@dataclass(frozen=True)
class Source:
    name: str
    archive_pattern: str
    article_patterns: tuple[re.Pattern[str], ...]
    enabled: bool = True
    note: str = ""

    def archive_url(self, day: date) -> str:
        return self.archive_pattern.format(
            iso=day.isoformat(), year=day.year, month=f"{day.month:02d}", day=f"{day.day:02d}"
        )

    def is_article_url(self, url: str) -> bool:
        parsed = urlparse(url)
        path = parsed.path
        return any(pattern.search(path) for pattern in self.article_patterns)


DEFAULT_SOURCES = {
    "kommersant": Source(
        "kommersant",
        "https://www.kommersant.ru/archive/news/day/{iso}",
        (re.compile(r"/doc/\d+"),),
    ),
    "interfax": Source(
        "interfax",
        "https://www.interfax.ru/news/{year}/{month}/{day}/",
        (re.compile(r"/(russia|world|business|moscow)/\d+$"),),
    ),
    "lenta": Source(
        "lenta",
        "https://lenta.ru/{year}/{month}/{day}/",
        (re.compile(r"/(news|articles|brief)/20\d{2}/\d{2}/\d{2}/"),),
    ),
    "vedomosti": Source(
        "vedomosti",
        "https://www.vedomosti.ru/archive/{year}/{month}/{day}",
        (re.compile(r"/(business|economics|finance|politics|society|technology)/(news|articles)/"),),
    ),
}


class LinkExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, str]] = []
        self._href: str | None = None
        self._buf: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "a":
            self._href = dict(attrs).get("href")
            self._buf = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._buf.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "a" and self._href:
            title = clean(" ".join(self._buf))
            if title:
                self.links.append((self._href, title))
            self._href = None
            self._buf = []


def clean(value: str) -> str:
    return re.sub(r"\s+", " ", unescape(value)).strip()


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc.lower(), parsed.path.rstrip("/"), "", "", ""))


def extract_url_date(url: str) -> date | None:
    match = re.search(r"/(20\d{2})/(\d{2})/(\d{2})(?:/|$)", urlparse(url).path)
    if not match:
        compact = re.search(r"/(20\d{2})(\d{2})(\d{2})(?:/|$)", urlparse(url).path)
        match = compact
    if not match:
        return None
    year, month, day = (int(part) for part in match.groups())
    return date(year, month, day)


def load_sources(path: Path) -> dict[str, Source]:
    if not path.exists():
        return DEFAULT_SOURCES

    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    sources: dict[str, Source] = {}
    for item in data.get("sources", []):
        name = item["name"]
        sources[name] = Source(
            name=name,
            archive_pattern=item["archive_pattern"],
            article_patterns=tuple(re.compile(pattern) for pattern in item.get("article_patterns", [])),
            enabled=bool(item.get("enabled", True)),
            note=item.get("note", ""),
        )
    return sources or DEFAULT_SOURCES


def day_range(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def fetch_html(url: str) -> str:
    resp = requests.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    return resp.text


def collect_titles(source: Source, day: date) -> list[dict[str, str]]:
    archive_url = source.archive_url(day)
    extractor = LinkExtractor()
    extractor.feed(fetch_html(archive_url))

    rows = []
    seen_urls: set[str] = set()
    for href, title in extractor.links:
        url = normalize_url(urljoin(archive_url, href))
        if url in seen_urls or not source.is_article_url(url):
            continue
        url_date = extract_url_date(url)
        if url_date is not None and url_date != day:
            continue
        if len(title) < 12 or len(title.split()) < 2:
            continue
        seen_urls.add(url)
        rows.append({"date": day.isoformat(), "source": source.name, "title": title, "url": url})
    return rows


def tokens(title: str) -> list[str]:
    words = re.findall(r"[A-Za-zА-Яа-яЁё0-9-]{3,}", title.lower())
    return [word for word in words if word not in STOPWORDS and not word.isdigit()]


def canonical_token(word: str) -> str:
    aliases = {
        "сша": "сша",
        "всу": "всу",
        "бпла": "бпла",
        "мид": "мид",
        "минобороны": "минобороны",
    }
    if word in aliases:
        return aliases[word]
    if not re.search(r"[а-яё]", word):
        return word
    for suffix in (
        "иями", "ями", "ами", "ого", "ему", "ыми", "ими", "ией", "ия", "иях",
        "ах", "ях", "ов", "ев", "ом", "ем", "ой", "ей", "ым", "им", "ую",
        "юю", "ая", "яя", "ое", "ее", "ые", "ие", "ый", "ий", "ого", "его",
        "а", "я", "ы", "и", "у", "ю", "е", "о",
    ):
        if len(word) - len(suffix) >= 4 and word.endswith(suffix):
            return word[: -len(suffix)]
    return word


def analysis_tokens(title: str) -> list[str]:
    return [canonical_token(word) for word in tokens(title)]


def normalize_title(title: str) -> str:
    words = tokens(title)
    return " ".join(words)


def fuzzy_dedup_key(title: str) -> str:
    words = sorted(set(analysis_tokens(title)))
    return " ".join(words[:10])


def classify(title: str) -> set[str]:
    low = title.lower()
    return {
        topic
        for topic, stems in TOPICS.items()
        if any(stem in low for stem in stems)
    } or {"other"}


def build_report(rows: list[dict[str, str]], start: date, end: date) -> str:
    by_source = Counter(row["source"] for row in rows)
    by_day = Counter(row["date"] for row in rows)
    topic_counts: Counter[str] = Counter()
    source_topic: dict[str, Counter[str]] = defaultdict(Counter)
    word_counts: Counter[str] = Counter()

    for row in rows:
        row_topics = classify(row["title"])
        topic_counts.update(row_topics)
        source_topic[row["source"]].update(row_topics)
        word_counts.update(tokens(row["title"]))

    last_week_start = max(start, end - timedelta(days=6))
    prev_week_start = max(start, last_week_start - timedelta(days=7))
    last_week = [row for row in rows if date.fromisoformat(row["date"]) >= last_week_start]
    prev_week = [
        row for row in rows
        if prev_week_start <= date.fromisoformat(row["date"]) < last_week_start
    ]
    last_topics = Counter(topic for row in last_week for topic in classify(row["title"]))
    prev_topics = Counter(topic for row in prev_week for topic in classify(row["title"]))

    acceleration = []
    for topic in sorted(set(last_topics) | set(prev_topics)):
        last_share = last_topics[topic] / max(1, len(last_week))
        prev_share = prev_topics[topic] / max(1, len(prev_week))
        acceleration.append((last_share - prev_share, topic, last_topics[topic], prev_topics[topic]))

    def bullet_counts(counter: Counter[str], limit: int = 8) -> list[str]:
        return [f"- `{name}`: {count}" for name, count in counter.most_common(limit)]

    lines = [
        "# News Pressure Snapshot",
        "",
        f"Period: `{start.isoformat()}` to `{end.isoformat()}`",
        f"Generated: `{datetime.now(UTC).isoformat(timespec='seconds')}`",
        f"Headlines: `{len(rows)}`",
        "",
        "## Sources",
        *bullet_counts(by_source),
        "",
        "## Dominant Topics",
        *bullet_counts(topic_counts),
        "",
        "## Rising Topics",
    ]
    for delta, topic, last_count, prev_count in sorted(acceleration, reverse=True)[:8]:
        lines.append(f"- `{topic}`: {prev_count} -> {last_count} ({delta:+.1%} share)")

    lines += [
        "",
        "## Frequent Words",
        *bullet_counts(word_counts, 20),
        "",
        "## Daily Volume",
    ]
    for day, count in sorted(by_day.items()):
        lines.append(f"- `{day}`: {count}")

    lines += ["", "## Source Topic Matrix"]
    for source in sorted(source_topic):
        compact = ", ".join(f"{topic}={count}" for topic, count in source_topic[source].most_common(6))
        lines.append(f"- `{source}`: {compact}")

    return "\n".join(lines) + "\n"


def init_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sources (
            name TEXT PRIMARY KEY,
            archive_pattern TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS headline_snapshots (
            id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            published_date TEXT NOT NULL,
            title TEXT NOT NULL,
            url TEXT NOT NULL UNIQUE,
            title_hash TEXT NOT NULL,
            collected_at TEXT NOT NULL,
            raw_meta TEXT NOT NULL DEFAULT '{}',
            FOREIGN KEY(source) REFERENCES sources(name)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshot_runs (
            run_id TEXT PRIMARY KEY,
            period_from TEXT NOT NULL,
            period_to TEXT NOT NULL,
            sources TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            fetched_count INTEGER NOT NULL DEFAULT 0,
            inserted_count INTEGER NOT NULL DEFAULT 0,
            error_count INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshot_errors (
            run_id TEXT NOT NULL,
            source TEXT NOT NULL,
            published_date TEXT NOT NULL,
            error TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS normalized_titles (
            article_id TEXT PRIMARY KEY,
            normalized_title TEXT NOT NULL,
            token_json TEXT NOT NULL,
            fuzzy_key TEXT NOT NULL,
            normalized_at TEXT NOT NULL,
            FOREIGN KEY(article_id) REFERENCES headline_snapshots(id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS topic_runs (
            run_id TEXT PRIMARY KEY,
            period_from TEXT NOT NULL,
            period_to TEXT NOT NULL,
            method TEXT NOT NULL,
            params_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS topics (
            topic_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            label TEXT NOT NULL,
            summary TEXT NOT NULL DEFAULT '',
            pressure_score REAL NOT NULL,
            volume INTEGER NOT NULL,
            velocity REAL NOT NULL,
            persistence INTEGER NOT NULL,
            source_spread INTEGER NOT NULL,
            novelty REAL NOT NULL,
            silence_gap REAL NOT NULL,
            representative_json TEXT NOT NULL,
            FOREIGN KEY(run_id) REFERENCES topic_runs(run_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS article_topics (
            article_id TEXT NOT NULL,
            topic_id TEXT NOT NULL,
            confidence REAL NOT NULL,
            PRIMARY KEY(article_id, topic_id),
            FOREIGN KEY(article_id) REFERENCES headline_snapshots(id),
            FOREIGN KEY(topic_id) REFERENCES topics(topic_id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_headlines_date ON headline_snapshots(published_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_headlines_source_date ON headline_snapshots(source, published_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_normalized_fuzzy ON normalized_titles(fuzzy_key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_topics_run_score ON topics(run_id, pressure_score DESC)")
    conn.commit()
    return conn


def stable_id(source: str, url: str) -> str:
    return hashlib.sha256(f"{source}\n{url}".encode("utf-8")).hexdigest()[:24]


def title_hash(title: str) -> str:
    norm = re.sub(r"\W+", " ", title.lower()).strip()
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()


def save_to_sqlite(
    db_path: Path,
    rows: list[dict[str, str]],
    errors: list[dict[str, str]],
    start: date,
    end: date,
    source_names: list[str],
    sources: dict[str, Source],
) -> tuple[str, int]:
    now = datetime.now(UTC).isoformat(timespec="seconds")
    run_id = "news_pressure_" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    conn = init_db(db_path)

    for name in source_names:
        source = sources[name]
        conn.execute(
            "INSERT OR REPLACE INTO sources(name, archive_pattern, enabled) VALUES (?, ?, ?)",
            (source.name, source.archive_pattern, 1 if source.enabled else 0),
        )

    conn.execute(
        """
        INSERT INTO snapshot_runs(run_id, period_from, period_to, sources, started_at, fetched_count, error_count)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (run_id, start.isoformat(), end.isoformat(), ",".join(source_names), now, len(rows), len(errors)),
    )

    inserted = 0
    for row in rows:
        article_id = stable_id(row["source"], row["url"])
        before = conn.total_changes
        conn.execute(
            """
            INSERT OR IGNORE INTO headline_snapshots(
                id, source, published_date, title, url, title_hash, collected_at, raw_meta
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                article_id,
                row["source"],
                row["date"],
                row["title"],
                row["url"],
                title_hash(row["title"]),
                now,
                json.dumps({"collector": "news_pressure_snapshot"}, ensure_ascii=False),
            ),
        )
        inserted += conn.total_changes - before
        row_tokens = analysis_tokens(row["title"])
        conn.execute(
            """
            INSERT OR REPLACE INTO normalized_titles(
                article_id, normalized_title, token_json, fuzzy_key, normalized_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                article_id,
                normalize_title(row["title"]),
                json.dumps(row_tokens, ensure_ascii=False),
                fuzzy_dedup_key(row["title"]),
                now,
            ),
        )

    for err in errors:
        conn.execute(
            "INSERT INTO snapshot_errors(run_id, source, published_date, error, created_at) VALUES (?, ?, ?, ?, ?)",
            (run_id, err["source"], err["date"], err["error"], now),
        )

    conn.execute(
        "UPDATE snapshot_runs SET finished_at=?, inserted_count=? WHERE run_id=?",
        (datetime.now(UTC).isoformat(timespec="seconds"), inserted, run_id),
    )
    conn.commit()
    conn.close()
    return run_id, inserted


def choose_cluster_label(row_tokens: list[str], doc_freq: Counter[str]) -> str:
    ranked = sorted(set(row_tokens), key=lambda word: (-doc_freq[word], word))
    if not ranked:
        return "other"
    return " / ".join(ranked[:2])


def cluster_rows(rows: list[dict[str, str]], start: date, end: date, limit: int = 15) -> list[dict[str, object]]:
    token_rows = [(row, analysis_tokens(row["title"])) for row in rows]
    doc_freq: Counter[str] = Counter()
    for _, row_tokens in token_rows:
        doc_freq.update(set(row_tokens))

    clustered: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row, row_tokens in token_rows:
        label = choose_cluster_label(row_tokens, doc_freq)
        clustered[label].append(row)

    last_week_start = max(start, end - timedelta(days=6))
    prev_week_start = max(start, last_week_start - timedelta(days=7))
    full_days = max(1, (end - start).days + 1)

    clusters: list[dict[str, object]] = []
    for label, items in clustered.items():
        if label == "other" or len(items) < 3:
            continue

        sources = {item["source"] for item in items}
        days = {item["date"] for item in items}
        last_count = sum(1 for item in items if date.fromisoformat(item["date"]) >= last_week_start)
        prev_count = sum(
            1 for item in items
            if prev_week_start <= date.fromisoformat(item["date"]) < last_week_start
        )
        last_share = last_count / max(1, len([r for r in rows if date.fromisoformat(r["date"]) >= last_week_start]))
        prev_share = prev_count / max(1, len([
            r for r in rows
            if prev_week_start <= date.fromisoformat(r["date"]) < last_week_start
        ]))
        velocity = last_share - prev_share
        volume = len(items)
        persistence = len(days)
        source_spread = len(sources)
        novelty = max(0.0, velocity)
        silence_gap = 1.0 - (source_spread / max(1, len({row["source"] for row in rows})))
        normalized_volume = volume / max(1, len(rows))
        pressure_score = (
            0.35 * normalized_volume
            + 0.25 * max(0.0, velocity)
            + 0.20 * (source_spread / max(1, len({row["source"] for row in rows})))
            + 0.10 * (persistence / full_days)
            + 0.10 * novelty
        )
        representatives = sorted(items, key=lambda item: item["date"], reverse=True)[:5]
        clusters.append({
            "label": label,
            "volume": volume,
            "velocity": velocity,
            "persistence": persistence,
            "source_spread": source_spread,
            "novelty": novelty,
            "silence_gap": silence_gap,
            "pressure_score": pressure_score,
            "representatives": representatives,
            "article_ids": [stable_id(item["source"], item["url"]) for item in items],
        })

    return sorted(clusters, key=lambda item: float(item["pressure_score"]), reverse=True)[:limit]


def save_clusters_to_sqlite(
    db_path: Path,
    clusters: list[dict[str, object]],
    start: date,
    end: date,
    method: str = "lexical_token_pair_v1",
) -> str:
    conn = init_db(db_path)
    run_id = "topic_" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    now = datetime.now(UTC).isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT INTO topic_runs(run_id, period_from, period_to, method, params_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            start.isoformat(),
            end.isoformat(),
            method,
            json.dumps({"cluster_limit": len(clusters)}, ensure_ascii=False),
            now,
        ),
    )
    for idx, cluster in enumerate(clusters, start=1):
        topic_id = f"{run_id}_{idx:03d}"
        representatives = cluster["representatives"]
        conn.execute(
            """
            INSERT INTO topics(
                topic_id, run_id, label, summary, pressure_score, volume, velocity,
                persistence, source_spread, novelty, silence_gap, representative_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                topic_id,
                run_id,
                cluster["label"],
                "Lexical baseline cluster; replace with embedding+LLM label in the next phase.",
                cluster["pressure_score"],
                cluster["volume"],
                cluster["velocity"],
                cluster["persistence"],
                cluster["source_spread"],
                cluster["novelty"],
                cluster["silence_gap"],
                json.dumps(representatives, ensure_ascii=False),
            ),
        )
        for article_id in cluster["article_ids"]:
            conn.execute(
                "INSERT OR IGNORE INTO article_topics(article_id, topic_id, confidence) VALUES (?, ?, ?)",
                (article_id, topic_id, 0.45),
            )
    conn.commit()
    conn.close()
    return run_id


def build_cluster_report(clusters: list[dict[str, object]], start: date, end: date, topic_run_id: str) -> str:
    lines = [
        "# News Pressure Cluster Report",
        "",
        f"Period: `{start.isoformat()}` to `{end.isoformat()}`",
        f"Topic run: `{topic_run_id}`",
        "Method: `lexical_token_pair_v1`",
        "",
        "This is a baseline clustering report. Treat labels as inspectable hints, not final semantics.",
        "",
    ]
    for idx, cluster in enumerate(clusters, start=1):
        lines += [
            f"## {idx}. {cluster['label']}",
            "",
            f"- pressure_score: `{float(cluster['pressure_score']):.4f}`",
            f"- volume: `{cluster['volume']}`",
            f"- velocity: `{float(cluster['velocity']):+.2%}`",
            f"- persistence: `{cluster['persistence']}` days",
            f"- source_spread: `{cluster['source_spread']}`",
            f"- silence_gap: `{float(cluster['silence_gap']):.2f}`",
            "- representatives:",
        ]
        for row in cluster["representatives"]:
            lines.append(f"  - `{row['date']}` `{row['source']}`: {row['title']} ({row['url']})")
        lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect archive headlines and build a news-pressure report.")
    parser.add_argument("--from", dest="from_date", help="Start date YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", help="End date YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=30, help="Number of days ending at --to/today")
    parser.add_argument("--sources", default="kommersant,interfax,lenta,vedomosti")
    parser.add_argument("--limit-per-source-day", type=int, default=80)
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="SQLite database path")
    parser.add_argument("--sources-config", default=str(DEFAULT_SOURCES_CONFIG), help="YAML source config path")
    parser.add_argument("--cluster-limit", type=int, default=15)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    end = date.fromisoformat(args.to_date) if args.to_date else date.today()
    start = date.fromisoformat(args.from_date) if args.from_date else end - timedelta(days=args.days - 1)
    sources = load_sources(Path(args.sources_config))
    requested_source_names = [name.strip() for name in args.sources.split(",") if name.strip()]
    source_names = []
    for name in requested_source_names:
        if name not in sources:
            raise SystemExit(f"Unknown source in config: {name}")
        if not sources[name].enabled:
            print(f"{name}: disabled ({sources[name].note or 'no note'})")
            continue
        source_names.append(name)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, str]] = []
    errors: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    for day in day_range(start, end):
        for name in source_names:
            source = sources[name]
            try:
                titles = collect_titles(source, day)[: args.limit_per_source_day]
                new_titles = [row for row in titles if row["url"] not in seen_urls]
                seen_urls.update(row["url"] for row in new_titles)
                rows.extend(new_titles)
                print(f"{day} {name}: {len(new_titles)} ({len(titles)} archive matches)")
            except Exception as exc:
                print(f"{day} {name}: ERROR {type(exc).__name__}: {exc}")
                errors.append({"date": day.isoformat(), "source": name, "error": f"{type(exc).__name__}: {exc}"})
            time.sleep(REQUEST_SLEEP_SECONDS)

    stamp = f"{start.isoformat()}_{end.isoformat()}"
    jsonl_path = OUT_DIR / f"headlines_{stamp}.jsonl"
    report_path = OUT_DIR / f"pulse_{stamp}.md"
    cluster_report_path = OUT_DIR / f"cluster_pulse_{stamp}.md"
    errors_path = OUT_DIR / f"errors_{stamp}.json"
    db_path = Path(args.db)

    with jsonl_path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    report_path.write_text(build_report(rows, start, end), encoding="utf-8")
    errors_path.write_text(json.dumps(errors, ensure_ascii=False, indent=2), encoding="utf-8")
    run_id, inserted = save_to_sqlite(db_path, rows, errors, start, end, source_names, sources)
    clusters = cluster_rows(rows, start, end, limit=args.cluster_limit)
    topic_run_id = save_clusters_to_sqlite(db_path, clusters, start, end)
    cluster_report_path.write_text(build_cluster_report(clusters, start, end, topic_run_id), encoding="utf-8")

    print(f"\nSaved {len(rows)} headlines")
    print(f"JSONL : {jsonl_path}")
    print(f"Report: {report_path}")
    print(f"Cluster report: {cluster_report_path}")
    print(f"Errors: {errors_path} ({len(errors)})")
    print(f"SQLite: {db_path} (run_id={run_id}, inserted={inserted})")
    print(f"Topic run: {topic_run_id} ({len(clusters)} clusters)")


if __name__ == "__main__":
    main()
