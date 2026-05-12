"""
en_news_archive_loader.py

Backfill English-language news archives (BBC, Guardian, Fox, AlJazeera, Euronews,
France24) into signal-mind ``db/hf_news.db`` using the same schema/run-id system
as ``ru_news_archive_loader.py``.

Target tables (created if absent, shared with the RU loader):

    articles(id, source, date, text, extra_fields, loaded_at)
    article_dedup(url_hash PRIMARY KEY, article_id, url, source, run_id, loaded_at)
    archive_load_runs(run_id PRIMARY KEY, status, db_path, date_from, date_to,
                      sources, dry_run, started_at, finished_at, stats_json)

Source field is namespaced as ``en_archive:<source>`` so RU/EN/HF rows coexist.

Filter:
    Keyword/topic dictionary from a YAML (default
    ``signal_mind_news_loader_source_only/config/news_keywords.yaml``).
    Matched keywords/topics are stored in ``extra_fields`` so downstream
    pipelines can re-filter without re-scanning text.

Safety:
    - dry-run by default; writes require --commit
    - --sandbox-db for separate test DB
    - strict publication-date validation (meta/JSON-LD must match archive day)
    - run_id for rollback (SQL template below)
    - article_dedup keyed on normalized URL for idempotent reruns

Examples:

    # 3-day sandbox pilot, BBC + Guardian, save matched articles
    .venv/Scripts/python en_news_archive_loader.py \\
      --sandbox-db db/hf_news_sandbox.db \\
      --from 2025-09-01 --to 2025-09-03 \\
      --sources bbc,guardian \\
      --commit --run-id en_archive_pilot_v1

    # Dry-run against real db
    .venv/Scripts/python en_news_archive_loader.py \\
      --from 2025-09-01 --to 2025-09-01 \\
      --sources bbc

    # Full backfill (all 6 sources)
    .venv/Scripts/python en_news_archive_loader.py \\
      --from 2025-09-01 --to today \\
      --sources bbc,guardian,fox,aljazeera,euronews,france24 \\
      --commit --run-id en_archive_full_v1

Rollback one run:

    DELETE FROM articles WHERE id IN (
        SELECT article_id FROM article_dedup WHERE run_id='RUN_ID'
    );
    DELETE FROM article_dedup WHERE run_id='RUN_ID';
    DELETE FROM archive_load_runs WHERE run_id='RUN_ID';
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import random
import re
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse, urlunparse

import requests
import yaml
from bs4 import BeautifulSoup
from dateutil.parser import parse as parse_date

try:
    import trafilatura
except Exception:
    trafilatura = None


ROOT = Path(__file__).resolve().parent
DEFAULT_DB_PATH = ROOT / "db" / "hf_news.db"
LOG_PATH = ROOT / "db" / "en_news_archive_loader.log"
DEFAULT_KEYWORDS = ROOT / "signal_mind_news_loader_source_only" / "config" / "news_keywords.yaml"

DEFAULT_SOURCES = ["bbc", "guardian", "fox", "aljazeera", "euronews", "france24"]
REQUEST_TIMEOUT = 60

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0 Safari/537.36"
)

MONTH_FULL = [None, "january", "february", "march", "april", "may", "june",
              "july", "august", "september", "october", "november", "december"]
MONTH_SHORT = [None, "jan", "feb", "mar", "apr", "may", "jun",
               "jul", "aug", "sep", "oct", "nov", "dec"]


@dataclass(frozen=True)
class ArchiveLink:
    source: str
    archive_date: date
    url: str
    title: str
    summary: str = ""


@dataclass(frozen=True)
class Article:
    source: str
    url: str
    archive_date: date
    published_at: datetime
    title: str
    body: str
    archive_title: str = ""
    archive_summary: str = ""

    @property
    def news_date(self) -> str:
        return self.published_at.date().isoformat()

    @property
    def text(self) -> str:
        return f"{self.title}\n\n{self.body}".strip()


def date_range(start: date, end: date) -> Iterable[date]:
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def parse_date_arg(value: str) -> date:
    if value.lower() == "today":
        return datetime.now(UTC).date()
    return date.fromisoformat(value)


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))


def sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="ignore")).hexdigest()


# ----- HTTP -----

class HttpClient:
    def __init__(
        self,
        sleep_seconds: float = 5.0,
        archive_sleep: float = 3.0,
        use_vpn: bool = False,
    ) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        self.sleep_seconds = sleep_seconds
        self.archive_sleep = archive_sleep
        self.use_vpn = use_vpn
        self._curl_socks_arg: list[str] = []
        if use_vpn:
            # Lazy import so the loader still works when the vpn module is
            # absent (e.g. minimal deployments).
            from src.utils.proxy import get_proxies, is_vpn_up

            self.session.proxies = get_proxies()
            # is_vpn_up implies start_vpn already ran; capture the SOCKS port
            # for the curl fallback path so it doesn't bypass the tunnel.
            assert is_vpn_up()
            socks_url = self.session.proxies["https"]  # socks5h://127.0.0.1:PORT
            host_port = socks_url.split("://", 1)[1]
            self._curl_socks_arg = ["--socks5-hostname", host_port]
            logging.info("HTTP client routing through VPN tunnel (%s)", socks_url)

    def _curl_fallback(self, url: str) -> str:
        # Browser-like headers + --compressed so euronews returns 200 instead
        # of 406. Explicit utf-8 decoding so non-ASCII titles don't blow up on
        # Windows cp1251 default codepage.
        cmd = [
            "curl", "-fsSL", "--compressed",
            "-A", USER_AGENT,
            "-H", "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "-H", "Accept-Language: en-US,en;q=0.5",
            "-H", "Upgrade-Insecure-Requests: 1",
            "-H", "Sec-Fetch-Dest: document",
            "-H", "Sec-Fetch-Mode: navigate",
            "-H", "Sec-Fetch-Site: none",
            *self._curl_socks_arg,
            url,
        ]
        cp = subprocess.run(
            cmd, capture_output=True, timeout=REQUEST_TIMEOUT,
            text=True, encoding="utf-8", errors="replace",
        )
        if cp.returncode != 0:
            raise requests.HTTPError(cp.stderr.strip() or f"curl failed for {url}")
        return cp.stdout

    def get(self, url: str, max_attempts: int = 3, jitter_base: float | None = None) -> str:
        jitter = jitter_base if jitter_base is not None else self.sleep_seconds
        time.sleep(jitter + random.uniform(0, max(jitter * 0.4, 0.5)))
        last_exc: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                r = self.session.get(url, timeout=REQUEST_TIMEOUT)
                if r.status_code in {403, 406} and (
                    "euronews.com" in url or "france24.com" in url
                ):
                    logging.debug("HTTP %s %d, retrying via curl", url, r.status_code)
                    return self._curl_fallback(url)
                r.raise_for_status()
                return r.text
            except (requests.ConnectionError, requests.Timeout) as e:
                last_exc = e
                if attempt >= max_attempts:
                    raise
                backoff = 2 ** attempt
                logging.warning("HTTP %s attempt %d/%d failed (%s); retry in %ds",
                                url, attempt, max_attempts, type(e).__name__, backoff)
                time.sleep(backoff)
            except requests.HTTPError as e:
                code = e.response.status_code if e.response is not None else 0
                if 500 <= code < 600 and attempt < max_attempts:
                    backoff = 2 ** attempt
                    logging.warning("HTTP %s %d attempt %d/%d; retry in %ds",
                                    url, code, attempt, max_attempts, backoff)
                    time.sleep(backoff)
                    continue
                raise
        if last_exc:
            raise last_exc
        raise RuntimeError("unreachable")


# ----- Source-specific archive parsing -----

def archive_url(source: str, day: date) -> str:
    if source == "bbc":
        return f"https://www.bbc.com/pages/content-index/{day:%Y/%m/%d}"
    if source == "guardian":
        return f"https://www.theguardian.com/world/{day.year}/{MONTH_SHORT[day.month]}/{day.day:02d}/all"
    if source == "fox":
        return f"https://www.foxnews.com/html-sitemap/{day.year}/{MONTH_FULL[day.month]}/{day.day}"
    if source == "aljazeera":
        return f"https://www.aljazeera.com/sitemap.xml?yyyy={day:%Y}&mm={day:%m}&dd={day:%d}"
    if source == "euronews":
        return f"https://www.euronews.com/{day:%Y/%m/%d}"
    if source == "france24":
        return f"https://www.france24.com/en/archives/{day:%Y/%m/%d}-{MONTH_FULL[day.month]}-{day.year}"
    raise ValueError(f"unknown source: {source}")


def parse_archive(source: str, day: date, html: str, base_url: str) -> list[ArchiveLink]:
    soup = BeautifulSoup(html, "xml" if source == "aljazeera" else "html.parser")
    out: list[ArchiveLink] = []
    seen: set[str] = set()

    if source == "aljazeera":
        for loc in soup.find_all("loc"):
            url = loc.get_text(" ", strip=True)
            parsed = urlparse(url)
            parts = [p for p in parsed.path.split("/") if p]
            if parsed.netloc not in {"www.aljazeera.com", "aljazeera.com"}:
                continue
            if len(parts) < 5 or parts[0] not in {"news", "economy", "features", "opinions"}:
                continue
            if parts[1:4] != [str(day.year), str(day.month), str(day.day)]:
                continue
            title = " ".join(parts[-1].replace("-", " ").split()).title()
            n = normalize_url(url)
            if n not in seen:
                seen.add(n)
                out.append(ArchiveLink(source, day, n, title))
        return out

    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        title = " ".join(a.get_text(" ", strip=True).split())
        if not title or len(title) < 8:
            continue
        url = normalize_url(urljoin(base_url, href))
        parsed = urlparse(url)
        if source == "bbc":
            if parsed.netloc not in {"www.bbc.com", "bbc.com"}:
                continue
            if "/articles/" not in parsed.path and "/news/live/" not in parsed.path:
                continue
        elif source == "guardian":
            if parsed.netloc not in {"www.theguardian.com", "theguardian.com"}:
                continue
            date_part = f"/{day.year}/{MONTH_SHORT[day.month]}/{day.day:02d}/"
            if date_part not in parsed.path:
                continue
            if parsed.path.endswith(f"/{day.year}/{MONTH_SHORT[day.month]}/{day.day:02d}/all"):
                continue
            if "/video/" in parsed.path or "/picture/" in parsed.path:
                continue
        elif source == "fox":
            if parsed.netloc not in {"www.foxnews.com", "foxnews.com"}:
                continue
            if "/html-sitemap/" in parsed.path:
                continue
            parts = [p for p in parsed.path.split("/") if p]
            if len(parts) < 2 or parts[0] in {"category", "person", "shows", "watch", "video"}:
                continue
        elif source == "euronews":
            if parsed.netloc not in {"www.euronews.com", "euronews.com"}:
                continue
            parts = [p for p in parsed.path.split("/") if p]
            if len(parts) < 4 or parts[:3] != [str(day.year), f"{day.month:02d}", f"{day.day:02d}"]:
                continue
            if parsed.path.rstrip("/") == f"/{day:%Y/%m/%d}":
                continue
        elif source == "france24":
            if parsed.netloc not in {"www.france24.com", "france24.com"}:
                continue
            if not re.search(rf"^/en/[^/]+/{day:%Y%m%d}-", parsed.path):
                continue
            if parsed.path.startswith(("/en/video/", "/en/tv-shows/")):
                continue
        if url in seen:
            continue
        seen.add(url)
        parent_text = " ".join((a.parent.get_text(" ", strip=True) if a.parent else "").split())
        summary = parent_text.replace(title, "", 1).strip()[:500]
        out.append(ArchiveLink(source, day, url, title, summary))
    return out


# ----- Article extraction -----

def extract_meta_date(soup: BeautifulSoup) -> str | None:
    keys = [
        {"property": "article:published_time"},
        {"name": "article:published_time"},
        {"name": "publishedDate"},          # Al Jazeera
        {"name": "publishdate"},
        {"name": "pubdate"},
        {"name": "date"},
        {"name": "dc.date"},
        {"name": "dc.date.issued"},
        {"itemprop": "datePublished"},
        {"property": "og:published_time"},
    ]
    for attrs in keys:
        tag = soup.find("meta", attrs=attrs)
        if tag and tag.get("content"):
            return tag["content"]
    t = soup.find("time")
    if t:
        return t.get("datetime") or t.get_text(" ", strip=True)
    return None


def extract_jsonld_date(soup: BeautifulSoup) -> str | None:
    """Scan <script type='application/ld+json'> for datePublished."""
    for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = s.get_text(" ", strip=True)
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            m = re.search(r'"datePublished"\s*:\s*"([^"]+)"', raw)
            if m:
                return m.group(1)
            continue

        def walk(o):
            if isinstance(o, dict):
                for k in ("datePublished", "dateCreated", "uploadDate"):
                    if k in o and isinstance(o[k], str):
                        return o[k]
                for v in o.values():
                    r = walk(v)
                    if r:
                        return r
            elif isinstance(o, list):
                for v in o:
                    r = walk(v)
                    if r:
                        return r
            return None

        found = walk(obj)
        if found:
            return found
    return None


def extract_article(url: str, html: str, archive_day: date) -> tuple[str, str, datetime | None]:
    soup = BeautifulSoup(html, "html.parser")
    title = ""
    for sel in ["h1", "meta[property='og:title']", "title"]:
        tag = soup.select_one(sel)
        if tag:
            t = tag.get("content") if tag.name == "meta" else tag.get_text(" ", strip=True)
            if t:
                title = t.strip()
                break

    body = ""
    if trafilatura:
        body = trafilatura.extract(html, url=url, include_comments=False, include_tables=False) or ""
    if not body:
        paras = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
        body = "\n".join(p for p in paras if len(p) > 40)
    body = body.strip()

    def _try_parse(raw: str | None) -> datetime | None:
        if not raw:
            return None
        try:
            dt = parse_date(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC)
        except Exception:
            return None

    published_at = _try_parse(extract_meta_date(soup))
    if published_at is None:
        published_at = _try_parse(extract_jsonld_date(soup))

    # Some sources include date in URL path; accept as fallback when meta missing.
    # Allow 1-2 digit month/day (Al Jazeera uses /YYYY/M/D/ without leading zeros).
    if published_at is None:
        m = re.search(r"/(20\d{2})[/-](\d{1,2})[/-](\d{1,2})", urlparse(url).path)
        if m:
            try:
                y, mo, d = (int(x) for x in m.groups())
                published_at = datetime(y, mo, d, tzinfo=UTC)
            except Exception:
                pass

    return title, body, published_at


def source_section(url: str) -> str:
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    host = parsed.netloc.replace("www.", "")
    if host == "france24.com":
        return parts[1] if len(parts) > 1 and parts[0] == "en" else (parts[0] if parts else "")
    if host == "euronews.com":
        return "news"
    return parts[0] if parts else ""


# ----- Keywords -----

def load_topic_keywords(path: Path, topics_arg: str) -> dict[str, list[str]]:
    if not path.exists():
        raise SystemExit(f"keywords file not found: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    topics = data.get("topics", {})
    selected = [t.strip() for t in topics_arg.split(",") if t.strip()]
    if selected == ["all"]:
        selected = list(topics.keys())
    result: dict[str, list[str]] = {}
    for topic in selected:
        block = topics.get(topic)
        if not block:
            raise SystemExit(f"unknown topic '{topic}' in {path}")
        values: list[str] = []
        if isinstance(block, dict):
            for nested in block.values():
                values.extend(nested or [])
        elif isinstance(block, list):
            values.extend(block)
        cleaned = [str(v).strip() for v in values if str(v).strip()]
        if cleaned:
            result[topic] = cleaned
    return result


def flatten_keywords(topic_keywords: dict[str, list[str]]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for vs in topic_keywords.values():
        for kw in vs:
            key = kw.lower()
            if key not in seen:
                seen.add(key)
                out.append(kw)
    return out


def find_matches(text: str, keywords: list[str]) -> list[str]:
    found: list[str] = []
    low_text = text.lower()
    for k in keywords:
        kk = k.lower().strip()
        if not kk:
            continue
        if re.fullmatch(r"[\w-]+", kk, flags=re.UNICODE):
            if re.search(rf"(?<![\w-]){re.escape(kk)}(?![\w-])", low_text, flags=re.UNICODE):
                found.append(k)
        elif kk in low_text:
            found.append(k)
    return found


def find_matched_topics(text: str, topic_keywords: dict[str, list[str]]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for topic, kws in topic_keywords.items():
        hits = sorted(set(find_matches(text, kws)))
        if hits:
            out[topic] = hits
    return out


# ----- DB -----

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
    return conn.execute(
        "SELECT 1 FROM article_dedup WHERE url_hash=?",
        (sha256(normalize_url(url)),),
    ).fetchone() is not None


def insert_article(conn: sqlite3.Connection, art: Article, run_id: str,
                   matched_keywords: list[str], matched_topics: dict[str, list[str]],
                   archive_matched: list[str], article_matched: list[str]) -> int:
    loaded_at = datetime.now(UTC).isoformat(timespec="seconds")
    extra = {
        "dataset": "en_news_archive",
        "origin": art.source,
        "url": art.url,
        "domain": urlparse(art.url).netloc,
        "title": art.title,
        "archive_title": art.archive_title,
        "section": source_section(art.url),
        "published_at": art.published_at.isoformat(),
        "language": "en",
        "parser": "en_news_archive_loader",
        "run_id": run_id,
        "matched_keywords": matched_keywords,
        "matched_topics": matched_topics,
        "archive_matched_keywords": archive_matched,
        "article_matched_keywords": article_matched,
    }
    cur = conn.execute(
        "INSERT INTO articles (source,date,text,extra_fields,loaded_at) VALUES (?,?,?,?,?)",
        (f"en_archive:{art.source}", art.news_date, art.text,
         json.dumps(extra, ensure_ascii=False), loaded_at),
    )
    aid = int(cur.lastrowid)
    conn.execute(
        "INSERT OR IGNORE INTO article_dedup (url_hash,article_id,url,source,run_id,loaded_at) VALUES (?,?,?,?,?,?)",
        (sha256(normalize_url(art.url)), aid, normalize_url(art.url), art.source, run_id, loaded_at),
    )
    return aid


def make_run_id() -> str:
    return "en_archive_" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def setup_logging(verbose: bool = False) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
        ],
    )


# ----- Main loop -----

def run(args: argparse.Namespace) -> dict:
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    unknown = [s for s in sources if s not in DEFAULT_SOURCES]
    if unknown:
        raise SystemExit(f"unknown sources: {unknown}; available: {DEFAULT_SOURCES}")

    day_from = parse_date_arg(args.date_from)
    day_to = parse_date_arg(args.date_to)
    if day_to < day_from:
        raise SystemExit("--to must be >= --from")

    keywords_path = Path(args.keywords_file).resolve()
    topic_keywords = load_topic_keywords(keywords_path, args.topics)
    keywords = flatten_keywords(topic_keywords)
    if not keywords:
        raise SystemExit("no keywords loaded")
    logging.info("loaded %d keywords across %d topics from %s",
                 len(keywords), len(topic_keywords), keywords_path)

    db_path = Path(args.sandbox_db or args.db).resolve()
    run_id = args.run_id or make_run_id()
    dry_run = not args.commit
    client = HttpClient(
        sleep_seconds=args.delay,
        archive_sleep=args.archive_delay,
        use_vpn=args.vpn,
    )
    conn = init_db(db_path)

    started_at = datetime.now(UTC).isoformat(timespec="seconds")
    conn.execute(
        "INSERT OR REPLACE INTO archive_load_runs (run_id,status,db_path,date_from,date_to,sources,dry_run,started_at) VALUES (?,?,?,?,?,?,?,?)",
        (run_id, "running", str(db_path), day_from.isoformat(), day_to.isoformat(),
         ",".join(sources), int(dry_run), started_at),
    )
    conn.commit()

    stats = {
        "run_id": run_id,
        "db_path": str(db_path),
        "dry_run": dry_run,
        "date_from": day_from.isoformat(),
        "date_to": day_to.isoformat(),
        "sources": sources,
        "match_scope": args.match_scope,
        "save_policy": args.save_policy,
        "days": 0,
        "archive_links": 0,
        "candidates": 0,
        "downloaded": 0,
        "saved": 0,
        "duplicates": 0,
        "rejected_no_date": 0,
        "rejected_date_mismatch": 0,
        "rejected_short": 0,
        "rejected_no_match": 0,
        "errors": [],
        "by_source": {s: {"links": 0, "candidates": 0, "downloaded": 0,
                          "saved": 0, "duplicates": 0, "errors": 0} for s in sources},
    }

    try:
        for day in date_range(day_from, day_to):
            stats["days"] += 1
            logging.info("=== %s ===", day.isoformat())
            for source in sources:
                au = archive_url(source, day)
                logging.info("%s: archive %s", source, au)
                try:
                    html = client.get(au, jitter_base=args.archive_delay)
                    links = parse_archive(source, day, html, au)
                except Exception as e:
                    msg = f"{source} {day}: archive error {type(e).__name__}: {e}"
                    logging.warning(msg)
                    stats["errors"].append(msg)
                    stats["by_source"][source]["errors"] += 1
                    continue

                stats["archive_links"] += len(links)
                stats["by_source"][source]["links"] += len(links)

                if args.match_scope == "archive":
                    candidates = [
                        x for x in links
                        if find_matches(f"{x.title}\n{x.summary}", keywords)
                    ]
                else:
                    candidates = links

                stats["candidates"] += len(candidates)
                stats["by_source"][source]["candidates"] += len(candidates)
                logging.info("%s %s: %d candidates / %d archive links",
                             source, day, len(candidates), len(links))

                for link in candidates:
                    if article_exists(conn, link.url):
                        stats["duplicates"] += 1
                        stats["by_source"][source]["duplicates"] += 1
                        continue
                    try:
                        ahtml = client.get(link.url, jitter_base=args.delay)
                        title, body, published_at = extract_article(link.url, ahtml, day)
                        stats["downloaded"] += 1
                        stats["by_source"][source]["downloaded"] += 1
                    except Exception as e:
                        msg = f"{source} {link.url}: parse error {type(e).__name__}: {e}"
                        logging.debug(msg)
                        stats["errors"].append(msg)
                        stats["by_source"][source]["errors"] += 1
                        continue

                    if published_at is None:
                        stats["rejected_no_date"] += 1
                        continue
                    if published_at.date() != day:
                        stats["rejected_date_mismatch"] += 1
                        continue
                    if not title:
                        title = link.title
                    if len(body) < 120:
                        stats["rejected_short"] += 1
                        continue

                    archive_text = f"{link.title}\n{link.summary}"
                    article_text = f"{title}\n{body}"
                    archive_matched = sorted(set(find_matches(archive_text, keywords)))
                    article_matched = sorted(set(find_matches(article_text, keywords)))
                    matched = sorted(set(archive_matched + article_matched))
                    topic_hits = find_matched_topics(f"{archive_text}\n{article_text}", topic_keywords)

                    if args.save_policy == "matched" and not matched:
                        stats["rejected_no_match"] += 1
                        continue

                    art = Article(
                        source=source, url=link.url, archive_date=day,
                        published_at=published_at, title=title, body=body,
                        archive_title=link.title, archive_summary=link.summary,
                    )

                    if not dry_run:
                        insert_article(conn, art, run_id, matched, topic_hits,
                                       archive_matched, article_matched)
                        conn.commit()
                    stats["saved"] += 1
                    stats["by_source"][source]["saved"] += 1
                    if stats["saved"] % 20 == 0:
                        logging.info("progress: saved=%d candidates=%d dups=%d",
                                     stats["saved"], stats["candidates"], stats["duplicates"])

                    if args.max_articles and stats["saved"] >= args.max_articles:
                        logging.info("max-articles reached: %d", stats["saved"])
                        status = "max_reached"
                        return _finalize(conn, run_id, stats, status)
        status = "dry_run_done" if dry_run else "done"
    except KeyboardInterrupt:
        status = "interrupted"
        raise
    except Exception:
        status = "error"
        raise
    finally:
        try:
            _finalize(conn, run_id, stats, locals().get("status", "error"))
        except Exception:
            pass

    return stats


def _finalize(conn: sqlite3.Connection, run_id: str, stats: dict, status: str) -> dict:
    finished_at = datetime.now(UTC).isoformat(timespec="seconds")
    conn.execute(
        "UPDATE archive_load_runs SET status=?, finished_at=?, stats_json=? WHERE run_id=?",
        (status, finished_at, json.dumps(stats, ensure_ascii=False), run_id),
    )
    conn.commit()
    conn.close()
    return stats


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Backfill English news archives into hf_news.db")
    p.add_argument("--db", default=str(DEFAULT_DB_PATH), help="Target hf_news.db path")
    p.add_argument("--sandbox-db", default=None, help="Use separate sandbox DB instead of --db")
    p.add_argument("--from", dest="date_from", required=True, help="Start date YYYY-MM-DD")
    p.add_argument("--to", dest="date_to", default="today", help="End date YYYY-MM-DD or today")
    p.add_argument("--sources", default=",".join(DEFAULT_SOURCES),
                   help="Comma-separated source names (subset of "
                        f"{','.join(DEFAULT_SOURCES)})")
    p.add_argument("--keywords-file", default=str(DEFAULT_KEYWORDS),
                   help="YAML topic dictionary")
    p.add_argument("--topics", default="all", help="Comma-separated topic names or 'all'")
    p.add_argument("--match-scope", choices=["archive", "article", "both"], default="article",
                   help="archive=filter by title/summary before download; "
                        "article=download every link then match full text")
    p.add_argument("--save-policy", choices=["matched", "all"], default="matched",
                   help="matched=save only keyword hits; all=save everything with tags")
    p.add_argument("--delay", type=float, default=5.0, help="Base delay between article requests")
    p.add_argument("--archive-delay", type=float, default=3.0, help="Delay between archive page fetches")
    p.add_argument("--max-articles", type=int, default=0, help="0 = unlimited; safety cap")
    p.add_argument("--commit", action="store_true", help="Actually write rows. Without this, dry-run only.")
    p.add_argument("--run-id", default=None, help="Explicit run id for rollback/audit")
    p.add_argument("--verbose", action="store_true", help="Debug logs")
    p.add_argument(
        "--vpn", action="store_true",
        help="Route HTTP through SSH SOCKS5 tunnel (src.utils.proxy). "
             "Use this when the local network blocks foreign news sources.",
    )
    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    setup_logging(args.verbose)
    stats = run(args)
    print(json.dumps(stats, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
