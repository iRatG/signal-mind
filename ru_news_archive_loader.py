"""
ru_news_archive_loader.py

Backfill Russian news archives into signal-mind `db/hf_news.db` without breaking
existing interfaces.

Target table is compatible with src/parsers/hf_news_loader.py:

    articles(id, source, date, text, extra_fields, loaded_at)

Safety features:
- default is dry-run; writes only with --commit
- sandbox mode via --sandbox-db
- strict publication-date validation before insert
- per-run run_id stored in extra_fields
- article_dedup table for idempotent reruns and rollback support
- source/date progress table

Examples:

    # Safe test: writes to separate sandbox db
    python -m src.parsers.ru_news_archive_loader \
      --sandbox-db db/hf_news_sandbox.db \
      --from 2025-10-01 --to 2025-10-01 \
      --sources kommersant,interfax \
      --limit-per-source 20 \
      --commit

    # Dry-run against real db: does not write
    python -m src.parsers.ru_news_archive_loader \
      --from 2025-10-01 --to 2025-10-03 \
      --sources kommersant,interfax

    # Real backfill
    python -m src.parsers.ru_news_archive_loader \
      --db db/hf_news.db \
      --from 2025-10-01 --to 2026-05-10 \
      --sources kommersant,interfax \
      --commit

Rollback one run:

    sqlite3 db/hf_news.db "
      DELETE FROM articles
      WHERE id IN (SELECT article_id FROM article_dedup WHERE run_id='RUN_ID');
      DELETE FROM article_dedup WHERE run_id='RUN_ID';
      DELETE FROM archive_load_runs WHERE run_id='RUN_ID';
    "

Notes:
- Full article text belongs to publishers; check terms/licensing for your usage.
- Vedomosti archive pages are dynamic/noisy; adapter is included but date validation
  may reject most links if static HTML contains current newsline instead of archive items.
"""

from __future__ import annotations

import argparse
import hashlib
import html as html_lib
import json
import logging
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse, urlunparse

import requests

ROOT = Path(__file__).resolve().parents[0]
DEFAULT_DB_PATH = ROOT / "db" / "hf_news.db"
LOG_PATH = ROOT / "db" / "ru_news_archive_loader.log"

DEFAULT_SOURCES = ["kommersant", "interfax", "lenta"]
REQUEST_TIMEOUT = 60
REQUEST_SLEEP_SECONDS = 0.4

USER_AGENT = (
    "Mozilla/5.0 (compatible; SignalMindRuArchiveLoader/0.1; "
    "+https://github.com/iRatG/signal-mind)"
)

RU_MONTHS = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}


@dataclass(frozen=True)
class Candidate:
    source: str
    url: str
    archive_date: date
    anchor_text: str = ""


@dataclass(frozen=True)
class Article:
    source: str
    url: str
    published_at: datetime
    title: str
    body: str
    section: str | None = None

    @property
    def news_date(self) -> str:
        return self.published_at.date().isoformat()

    @property
    def text(self) -> str:
        return f"{self.title}\n\n{self.body}".strip()


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
            self.links.append((self._href, clean(" ".join(self._buf))))
            self._href = None
            self._buf = []


class ArticleTextExtractor(HTMLParser):
    """Small dependency-free extractor: h1/title/meta/time/p text."""

    def __init__(self) -> None:
        super().__init__()
        self.h1: list[str] = []
        self.title_tag: list[str] = []
        self.paragraphs: list[str] = []
        self.times: list[str] = []
        self.meta: dict[str, str] = {}
        self.json_ld: list[str] = []

        self._in_h1 = False
        self._in_title = False
        self._in_p = False
        self._in_time = False
        self._in_json_ld = False
        self._p_buf: list[str] = []
        self._time_buf: list[str] = []
        self._json_buf: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        a = {k.lower(): v or "" for k, v in attrs}

        if t == "h1":
            self._in_h1 = True
        elif t == "title":
            self._in_title = True
        elif t == "p":
            self._in_p = True
            self._p_buf = []
        elif t == "time":
            self._in_time = True
            self._time_buf = []
            if a.get("datetime"):
                self.times.append(a["datetime"])
        elif t == "script" and a.get("type", "").lower() == "application/ld+json":
            self._in_json_ld = True
            self._json_buf = []
        elif t == "meta":
            key = a.get("property") or a.get("name") or a.get("itemprop")
            val = a.get("content")
            if key and val:
                self.meta[key] = val

    def handle_data(self, data: str) -> None:
        if self._in_h1:
            self.h1.append(data)
        if self._in_title:
            self.title_tag.append(data)
        if self._in_p:
            self._p_buf.append(data)
        if self._in_time:
            self._time_buf.append(data)
        if self._in_json_ld:
            self._json_buf.append(data)

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()
        if t == "h1":
            self._in_h1 = False
        elif t == "title":
            self._in_title = False
        elif t == "p" and self._in_p:
            txt = clean(" ".join(self._p_buf))
            if len(txt) >= 40 and not looks_like_boilerplate(txt):
                self.paragraphs.append(txt)
            self._in_p = False
            self._p_buf = []
        elif t == "time" and self._in_time:
            txt = clean(" ".join(self._time_buf))
            if txt:
                self.times.append(txt)
            self._in_time = False
            self._time_buf = []
        elif t == "script" and self._in_json_ld:
            raw = "".join(self._json_buf).strip()
            if raw:
                self.json_ld.append(raw)
            self._in_json_ld = False
            self._json_buf = []


def clean(value: str | None) -> str:
    return re.sub(r"\s+", " ", html_lib.unescape(value or "")).strip()


def looks_like_boilerplate(text: str) -> bool:
    low = text.lower()
    bad = [
        "использует файлы cookie",
        "все права защищены",
        "сетевое издание",
        "рекламные материалы",
        "правила перепечатки",
        "настоящий сайт",
        "подписывайтесь на",
    ]
    return any(x in low for x in bad)


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    # Keep query only when needed? For dedupe, drop tracking query params.
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))


def sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="ignore")).hexdigest()


def date_range(start: date, end: date) -> Iterable[date]:
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def parse_date_arg(value: str) -> date:
    if value.lower() == "today":
        return datetime.now(UTC).date()
    return date.fromisoformat(value)


def parse_any_datetime(value: str | None, fallback_year: int | None = None) -> datetime | None:
    raw = clean(value)
    if not raw:
        return None

    # ISO-ish dates: 2025-10-01T12:34:56+03:00 or 2025-10-01
    m = re.search(r"(20\d{2})[-/.](\d{1,2})[-/.](\d{1,2})(?:[T\s](\d{1,2}):(\d{2})(?::(\d{2}))?)?", raw)
    if m:
        y, mo, d, hh, mm, ss = m.groups()
        return datetime(
            int(y), int(mo), int(d), int(hh or 0), int(mm or 0), int(ss or 0), tzinfo=UTC
        )

    # Russian date: 1 октября 2025, 14:30
    m = re.search(
        r"(\d{1,2})\s+([а-яё]+)\s+(20\d{2})(?:[,\s]+(\d{1,2}):(\d{2}))?",
        raw.lower(),
    )
    if m:
        d, mon, y, hh, mm = m.groups()
        if mon in RU_MONTHS:
            return datetime(int(y), RU_MONTHS[mon], int(d), int(hh or 0), int(mm or 0), tzinfo=UTC)

    # Short Russian date: 01.10, 14:30 (rare; needs fallback_year)
    if fallback_year:
        m = re.search(r"(\d{1,2})\.(\d{1,2})(?:[,\s]+(\d{1,2}):(\d{2}))?", raw)
        if m:
            d, mo, hh, mm = m.groups()
            return datetime(fallback_year, int(mo), int(d), int(hh or 0), int(mm or 0), tzinfo=UTC)

    # RFC822 from RSS/meta, if encountered.
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        return None


def dates_from_json_ld(json_ld_items: list[str]) -> list[datetime]:
    found: list[datetime] = []

    def walk(obj) -> None:
        if isinstance(obj, dict):
            for key in ("datePublished", "dateCreated", "uploadDate", "dateModified"):
                if key in obj:
                    dt = parse_any_datetime(str(obj[key]))
                    if dt:
                        found.append(dt)
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for v in obj:
                walk(v)

    for raw in json_ld_items:
        try:
            walk(json.loads(raw))
        except Exception:
            # Some pages contain invalid JSON-LD; regex fallback.
            for m in re.findall(r'"date(?:Published|Created|Modified)"\s*:\s*"([^"]+)"', raw):
                dt = parse_any_datetime(m)
                if dt:
                    found.append(dt)
    return found


def published_date_from_url(source: str, url: str) -> datetime | None:
    path = urlparse(url).path
    # Vedomosti URLs usually contain /YYYY/MM/DD/.
    m = re.search(r"/(20\d{2})/(\d{2})/(\d{2})/", path)
    if m:
        y, mo, d = m.groups()
        return datetime(int(y), int(mo), int(d), tzinfo=UTC)
    # Interfax numeric article URLs do not expose date. Kommersant /doc/id does not either.
    return None


def choose_published_at(source: str, url: str, parsed: ArticleTextExtractor, archive_day: date) -> datetime | None:
    candidates: list[datetime] = []

    for key in [
        "article:published_time",
        "datePublished",
        "dateCreated",
        "pubdate",
        "publishdate",
        "sailthru.date",
        "og:published_time",
    ]:
        dt = parse_any_datetime(parsed.meta.get(key), fallback_year=archive_day.year)
        if dt:
            candidates.append(dt)

    candidates.extend(dates_from_json_ld(parsed.json_ld))

    for item in parsed.times:
        dt = parse_any_datetime(item, fallback_year=archive_day.year)
        if dt:
            candidates.append(dt)

    dt_url = published_date_from_url(source, url)
    if dt_url:
        candidates.append(dt_url)

    # Prefer exact archive-day matches; many pages also contain unrelated current timestamps.
    exact = [dt for dt in candidates if dt.date() == archive_day]
    if exact:
        return exact[0]

    # If no exact match, do NOT trust archive date implicitly. Return best candidate only
    # for diagnostics; caller will reject it by date validation.
    return candidates[0] if candidates else None


def setup_logging(verbose: bool = False) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_PATH, encoding="utf-8")],
    )


class HttpClient:
    def __init__(self, sleep_seconds: float = REQUEST_SLEEP_SECONDS) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.sleep_seconds = sleep_seconds

    def get(self, url: str, max_attempts: int = 3) -> str:
        time.sleep(self.sleep_seconds)
        for attempt in range(1, max_attempts + 1):
            try:
                r = self.session.get(url, timeout=REQUEST_TIMEOUT)
                r.raise_for_status()
                return r.text
            except (requests.ConnectionError, requests.Timeout) as e:
                if attempt >= max_attempts:
                    raise
                backoff = 2 ** attempt
                logging.warning(
                    "HTTP %s attempt %d/%d failed (%s); retry in %ds",
                    url, attempt, max_attempts, type(e).__name__, backoff,
                )
                time.sleep(backoff)
            except requests.HTTPError as e:
                code = e.response.status_code if e.response is not None else 0
                if 500 <= code < 600 and attempt < max_attempts:
                    backoff = 2 ** attempt
                    logging.warning(
                        "HTTP %s %d attempt %d/%d; retry in %ds",
                        url, code, attempt, max_attempts, backoff,
                    )
                    time.sleep(backoff)
                    continue
                raise
        raise RuntimeError("unreachable")


class BaseSource:
    name = "base"
    domain = ""

    def archive_url(self, day: date) -> str:
        raise NotImplementedError

    def is_article_url(self, url: str) -> bool:
        raise NotImplementedError

    def candidates(self, client: HttpClient, day: date) -> list[Candidate]:
        url = self.archive_url(day)
        html = client.get(url)
        extractor = LinkExtractor()
        extractor.feed(html)
        result: list[Candidate] = []
        seen: set[str] = set()
        for href, anchor in extractor.links:
            full = normalize_url(urljoin(url, href))
            if full in seen:
                continue
            if self.is_article_url(full):
                seen.add(full)
                result.append(Candidate(self.name, full, day, anchor))
        return result

    def parse_article(self, client: HttpClient, cand: Candidate) -> Article | None:
        html = client.get(cand.url)
        parsed = ArticleTextExtractor()
        parsed.feed(html)

        title = clean(" ".join(parsed.h1)) or clean(parsed.meta.get("og:title"))
        if not title:
            title = clean(" ".join(parsed.title_tag))
        title = re.sub(r"\s*[—|-]\s*(Коммерсантъ|Ведомости|Интерфакс|Lenta\.ru|Лента\.ру).*$", "", title).strip()

        body = "\n".join(unique_preserve_order(parsed.paragraphs))
        published_at = choose_published_at(self.name, cand.url, parsed, cand.archive_date)

        if not published_at:
            logging.debug("%s: no published date: %s", self.name, cand.url)
            return None
        if published_at.date() != cand.archive_date:
            logging.debug(
                "%s: reject date mismatch archive=%s published=%s url=%s",
                self.name,
                cand.archive_date,
                published_at.date(),
                cand.url,
            )
            return None
        if not title or len(body) < 120:
            logging.debug("%s: reject short article title=%r body_len=%d url=%s", self.name, title, len(body), cand.url)
            return None

        return Article(self.name, cand.url, published_at, title, body, section=None)


class KommersantSource(BaseSource):
    name = "kommersant"
    domain = "www.kommersant.ru"

    def archive_url(self, day: date) -> str:
        return f"https://www.kommersant.ru/archive/news/day/{day.isoformat()}"

    def is_article_url(self, url: str) -> bool:
        parsed = urlparse(url)
        return parsed.netloc.endswith("kommersant.ru") and re.search(r"/doc/\d+", parsed.path) is not None


class InterfaxSource(BaseSource):
    name = "interfax"
    domain = "www.interfax.ru"

    def archive_url(self, day: date) -> str:
        return f"https://www.interfax.ru/news/{day:%Y/%m/%d}/"

    def is_article_url(self, url: str) -> bool:
        parsed = urlparse(url)
        if not parsed.netloc.endswith("interfax.ru"):
            return False
        return re.search(r"/(russia|world|business|moscow)/\d+$", parsed.path) is not None


class VedomostiSource(BaseSource):
    name = "vedomosti"
    domain = "www.vedomosti.ru"

    def archive_url(self, day: date) -> str:
        return f"https://www.vedomosti.ru/archive/{day:%Y/%m/%d}"

    def is_article_url(self, url: str) -> bool:
        parsed = urlparse(url)
        if not parsed.netloc.endswith("vedomosti.ru"):
            return False
        return re.search(
            r"/(business|economics|finance|politics|society|technology|management|opinion|realty)/(news|articles|columns)/(20\d{2})/(\d{2})/(\d{2})/",
            parsed.path,
        ) is not None


class LentaSource(BaseSource):
    name = "lenta"
    domain = "lenta.ru"

    def archive_url(self, day: date) -> str:
        return f"https://lenta.ru/{day:%Y/%m/%d}/"

    def is_article_url(self, url: str) -> bool:
        parsed = urlparse(url)
        if not parsed.netloc.endswith("lenta.ru"):
            return False
        return re.search(r"/(news|articles|brief)/(20\d{2})/(\d{2})/(\d{2})/", parsed.path) is not None


SOURCES: dict[str, BaseSource] = {
    "kommersant": KommersantSource(),
    "interfax": InterfaxSource(),
    "vedomosti": VedomostiSource(),
    "lenta": LentaSource(),
}


def unique_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        norm = item.lower()[:160]
        if norm not in seen:
            seen.add(norm)
            out.append(item)
    return out


def init_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            source       TEXT,
            date         TEXT,
            text         TEXT,
            extra_fields TEXT,
            loaded_at    TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_date ON articles(date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_source ON articles(source)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS article_dedup (
            url_hash   TEXT PRIMARY KEY,
            article_id INTEGER,
            url        TEXT,
            source     TEXT,
            run_id     TEXT,
            loaded_at  TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_article_dedup_run ON article_dedup(run_id)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS archive_load_runs (
            run_id       TEXT PRIMARY KEY,
            status       TEXT,
            db_path      TEXT,
            date_from    TEXT,
            date_to      TEXT,
            sources      TEXT,
            dry_run      INTEGER,
            started_at   TEXT,
            finished_at  TEXT,
            stats_json   TEXT
        )
    """)
    conn.commit()
    return conn


def article_exists(conn: sqlite3.Connection, url: str) -> bool:
    return conn.execute("SELECT 1 FROM article_dedup WHERE url_hash=?", (sha256(normalize_url(url)),)).fetchone() is not None


def insert_article(conn: sqlite3.Connection, article: Article, run_id: str) -> int:
    loaded_at = datetime.now(UTC).isoformat(timespec="seconds")
    extra_fields = json.dumps(
        {
            "dataset": "ru_news_archive",
            "origin": article.source,
            "url": article.url,
            "domain": urlparse(article.url).netloc,
            "title": article.title,
            "published_at": article.published_at.isoformat(),
            "language": "ru",
            "parser": "ru_news_archive_loader",
            "run_id": run_id,
        },
        ensure_ascii=False,
    )
    cur = conn.execute(
        "INSERT INTO articles (source,date,text,extra_fields,loaded_at) VALUES (?,?,?,?,?)",
        (f"ru_archive:{article.source}", article.news_date, article.text, extra_fields, loaded_at),
    )
    article_id = int(cur.lastrowid)
    conn.execute(
        "INSERT OR IGNORE INTO article_dedup (url_hash,article_id,url,source,run_id,loaded_at) VALUES (?,?,?,?,?,?)",
        (sha256(normalize_url(article.url)), article_id, normalize_url(article.url), article.source, run_id, loaded_at),
    )
    return article_id


def make_run_id() -> str:
    return "ru_archive_" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def run(args: argparse.Namespace) -> dict:
    selected_sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    unknown = [s for s in selected_sources if s not in SOURCES]
    if unknown:
        raise SystemExit(f"Unknown sources: {unknown}. Available: {sorted(SOURCES)}")

    db_path = Path(args.sandbox_db or args.db).resolve()
    day_from = parse_date_arg(args.date_from)
    day_to = parse_date_arg(args.date_to)
    if day_to < day_from:
        raise SystemExit("--to must be >= --from")

    run_id = args.run_id or make_run_id()
    dry_run = not args.commit
    client = HttpClient(sleep_seconds=args.sleep)
    conn = init_db(db_path)

    started_at = datetime.now(UTC).isoformat(timespec="seconds")
    conn.execute(
        "INSERT OR REPLACE INTO archive_load_runs (run_id,status,db_path,date_from,date_to,sources,dry_run,started_at) VALUES (?,?,?,?,?,?,?,?)",
        (run_id, "running", str(db_path), day_from.isoformat(), day_to.isoformat(), ",".join(selected_sources), int(dry_run), started_at),
    )
    conn.commit()

    stats = {
        "run_id": run_id,
        "db_path": str(db_path),
        "dry_run": dry_run,
        "date_from": day_from.isoformat(),
        "date_to": day_to.isoformat(),
        "sources": selected_sources,
        "days": 0,
        "candidates": 0,
        "parsed_valid": 0,
        "inserted": 0,
        "duplicates": 0,
        "errors": [],
        "by_source": {s: {"candidates": 0, "valid": 0, "inserted": 0, "duplicates": 0, "errors": 0} for s in selected_sources},
    }

    try:
        for day in date_range(day_from, day_to):
            stats["days"] += 1
            logging.info("=== %s ===", day.isoformat())
            for source_name in selected_sources:
                source = SOURCES[source_name]
                logging.info("%s: archive %s", source_name, source.archive_url(day))
                try:
                    candidates = source.candidates(client, day)
                except Exception as e:
                    msg = f"{source_name} {day}: archive error {type(e).__name__}: {e}"
                    logging.warning(msg)
                    stats["errors"].append(msg)
                    stats["by_source"][source_name]["errors"] += 1
                    continue

                if args.limit_per_source:
                    candidates = candidates[: args.limit_per_source]
                stats["candidates"] += len(candidates)
                stats["by_source"][source_name]["candidates"] += len(candidates)
                logging.info("%s: %d candidate links", source_name, len(candidates))

                for cand in candidates:
                    if article_exists(conn, cand.url):
                        stats["duplicates"] += 1
                        stats["by_source"][source_name]["duplicates"] += 1
                        continue
                    try:
                        article = source.parse_article(client, cand)
                    except Exception as e:
                        msg = f"{source_name} {cand.url}: parse error {type(e).__name__}: {e}"
                        logging.debug(msg)
                        stats["errors"].append(msg)
                        stats["by_source"][source_name]["errors"] += 1
                        continue

                    if not article:
                        continue

                    stats["parsed_valid"] += 1
                    stats["by_source"][source_name]["valid"] += 1
                    logging.info("VALID %s %s %s", source_name, article.news_date, article.title[:90])

                    if not dry_run:
                        insert_article(conn, article, run_id)
                        conn.commit()
                        stats["inserted"] += 1
                        stats["by_source"][source_name]["inserted"] += 1

        status = "dry_run_done" if dry_run else "done"
    except KeyboardInterrupt:
        status = "interrupted"
        raise
    except Exception:
        status = "error"
        raise
    finally:
        finished_at = datetime.now(UTC).isoformat(timespec="seconds")
        try:
            conn.execute(
                "UPDATE archive_load_runs SET status=?, finished_at=?, stats_json=? WHERE run_id=?",
                (status, finished_at, json.dumps(stats, ensure_ascii=False), run_id),
            )
            conn.commit()
            conn.close()
        except Exception:
            pass

    return stats


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Backfill Russian news archives into hf_news.db")
    p.add_argument("--db", default=str(DEFAULT_DB_PATH), help="Target hf_news.db path")
    p.add_argument("--sandbox-db", default=None, help="Use separate sandbox DB instead of --db")
    p.add_argument("--from", dest="date_from", required=True, help="Start date YYYY-MM-DD")
    p.add_argument("--to", dest="date_to", default="today", help="End date YYYY-MM-DD or today")
    p.add_argument("--sources", default=",".join(DEFAULT_SOURCES), help="Comma-separated source names")
    p.add_argument("--limit-per-source", type=int, default=0, help="Limit candidates per source/day; 0 = no limit")
    p.add_argument("--commit", action="store_true", help="Actually write rows. Without this, dry-run only.")
    p.add_argument("--run-id", default=None, help="Explicit run id for rollback/audit")
    p.add_argument("--sleep", type=float, default=REQUEST_SLEEP_SECONDS, help="Delay between HTTP requests")
    p.add_argument("--verbose", action="store_true", help="Debug logs")
    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    setup_logging(args.verbose)
    stats = run(args)
    print(json.dumps(stats, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
