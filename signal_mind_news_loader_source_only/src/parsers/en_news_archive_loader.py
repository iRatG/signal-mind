#!/usr/bin/env python3
"""Small personal news archive loader.

Fetches daily archive pages, finds article links matching keywords, then downloads
article text and saves JSONL rows. Intended for low-rate personal research use.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import random
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse

VENDOR = Path(__file__).resolve().parent / "vendor"
if VENDOR.exists():
    sys.path.insert(0, str(VENDOR))

import requests
from bs4 import BeautifulSoup

try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None

try:
    from dateutil.parser import parse as parse_date
except Exception:  # pragma: no cover
    parse_date = None

try:
    import trafilatura
except Exception:
    trafilatura = None

USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
MONTH_FULL = [None, "january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"]
MONTH_SHORT = [None, "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]


@dataclass
class ArchiveLink:
    source: str
    date: str
    url: str
    title: str
    summary: str = ""


def daterange(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    cur = start
    while cur <= end:
        yield cur
        cur += dt.timedelta(days=1)


def fetch(url: str, timeout: int = 30) -> str:
    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.9"}
    r = requests.get(url, headers=headers, timeout=timeout)
    if (r.status_code == 406 and "euronews.com" in url) or (r.status_code == 403 and "france24.com" in url):
        # Some sites reject python-requests from this VPS while accepting curl.
        cp = subprocess.run(["curl", "-fsSL", url], text=True, capture_output=True, timeout=timeout)
        if cp.returncode != 0:
            raise requests.HTTPError(cp.stderr.strip() or f"curl failed for {url}")
        return cp.stdout
    r.raise_for_status()
    return r.text


def archive_url(source: str, day: dt.date) -> str:
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


def parse_archive(source: str, day: dt.date, html: str, base_url: str) -> list[ArchiveLink]:
    soup = BeautifulSoup(html, "xml" if source == "aljazeera" else "html.parser")
    links: list[ArchiveLink] = []
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
            if url not in seen:
                seen.add(url)
                links.append(ArchiveLink(source=source, date=day.isoformat(), url=url, title=title))
        return links

    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        title = " ".join(a.get_text(" ", strip=True).split())
        if not title or len(title) < 8:
            continue
        url = urljoin(base_url, href)
        parsed = urlparse(url)
        if source == "bbc":
            if parsed.netloc not in {"www.bbc.com", "bbc.com"}:
                continue
            # BBC content index contains section navigation too; keep actual story/live URLs.
            if "/articles/" not in parsed.path and "/news/live/" not in parsed.path:
                continue
        elif source == "guardian":
            if parsed.netloc not in {"www.theguardian.com", "theguardian.com"}:
                continue
            # Keep only actual items from this day's archive, not nav/section links.
            date_part = f"/{day.year}/{MONTH_SHORT[day.month]}/{day.day:02d}/"
            if date_part not in parsed.path:
                continue
            if parsed.path.endswith(f"/{day.year}/{MONTH_SHORT[day.month]}/{day.day:02d}/all") or parsed.fragment:
                continue
            if "/video/" in parsed.path or "/picture/" in parsed.path:
                continue
        elif source == "fox":
            if parsed.netloc not in {"www.foxnews.com", "foxnews.com"}:
                continue
            if "/html-sitemap/" in parsed.path:
                continue
            parts = [p for p in parsed.path.split("/") if p]
            # Fox sitemap includes nav/category/person pages; articles are normally /section/slug.
            if len(parts) < 2 or parts[0] in {"category", "person", "shows", "watch", "video"}:
                continue
        elif source == "euronews":
            if parsed.netloc not in {"www.euronews.com", "euronews.com"}:
                continue
            parts = [p for p in parsed.path.split("/") if p]
            if len(parts) < 4 or parts[:3] != [str(day.year), f"{day.month:02d}", f"{day.day:02d}"]:
                continue
            # Skip tag/category/index noise; daily archive articles are /YYYY/MM/DD/slug.
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
        # Nearby text often includes BBC/Guardian summary.
        parent_text = " ".join((a.parent.get_text(" ", strip=True) if a.parent else "").split())
        summary = parent_text.replace(title, "", 1).strip()[:500]
        links.append(ArchiveLink(source=source, date=day.isoformat(), url=url, title=title, summary=summary))
    return links


def load_keyword_topics(keywords_file: str | None, topics_arg: str) -> dict[str, list[str]]:
    """Load YAML topic dictionary as {topic: keywords}."""
    result: dict[str, list[str]] = {}
    if not keywords_file:
        return result
    if yaml is None:
        raise RuntimeError("PyYAML is required for --keywords-file")
    data = yaml.safe_load(Path(keywords_file).read_text(encoding="utf-8")) or {}
    topics = data.get("topics", {})
    selected = [t.strip() for t in topics_arg.split(",") if t.strip()]
    if selected == ["all"]:
        selected = list(topics.keys())
    for topic in selected:
        block = topics.get(topic)
        if not block:
            raise ValueError(f"Unknown topic '{topic}' in {keywords_file}")
        values: list[str] = []
        if isinstance(block, dict):
            for nested in block.values():
                values.extend(nested or [])
        elif isinstance(block, list):
            values.extend(block)
        result[topic] = [str(v).strip() for v in values if str(v).strip()]
    return result


def flatten_keywords(topic_keywords: dict[str, list[str]], keywords_arg: str | None = None) -> list[str]:
    result: list[str] = []
    for values in topic_keywords.values():
        result.extend(values)
    if keywords_arg:
        result.extend(k.strip() for k in keywords_arg.split(",") if k.strip())
    seen: set[str] = set()
    deduped: list[str] = []
    for kw in result:
        kw = str(kw).strip()
        key = kw.lower()
        if kw and key not in seen:
            seen.add(key)
            deduped.append(kw)
    return deduped


def load_keywords(keywords_arg: str | None, keywords_file: str | None, topics_arg: str) -> list[str]:
    """Load keywords from CLI and/or YAML topic dictionary."""
    result: list[str] = []
    if keywords_file:
        if yaml is None:
            raise RuntimeError("PyYAML is required for --keywords-file")
        data = yaml.safe_load(Path(keywords_file).read_text(encoding="utf-8")) or {}
        topics = data.get("topics", {})
        selected = [t.strip() for t in topics_arg.split(",") if t.strip()]
        if selected == ["all"]:
            selected = list(topics.keys())
        for topic in selected:
            block = topics.get(topic)
            if not block:
                raise ValueError(f"Unknown topic '{topic}' in {keywords_file}")
            if isinstance(block, dict):
                for values in block.values():
                    result.extend(values or [])
            elif isinstance(block, list):
                result.extend(block)
    if keywords_arg:
        result.extend(k.strip() for k in keywords_arg.split(",") if k.strip())
    seen: set[str] = set()
    deduped: list[str] = []
    for kw in result:
        kw = str(kw).strip()
        key = kw.lower()
        if kw and key not in seen:
            seen.add(key)
            deduped.append(kw)
    return deduped


def matches(text: str, keywords: list[str]) -> list[str]:
    # Match whole words for simple alphanumeric keywords, so "ai" doesn't hit
    # "said", "chair", "again", etc. Phrases still work as case-insensitive substrings.
    found: list[str] = []
    for k in keywords:
        kk = k.lower().strip()
        if not kk:
            continue
        if re.fullmatch(r"[\w-]+", kk, flags=re.UNICODE):
            if re.search(rf"(?<![\w-]){re.escape(kk)}(?![\w-])", text, flags=re.IGNORECASE | re.UNICODE):
                found.append(k)
        elif kk in text.lower():
            found.append(k)
    return found


def matched_topics(text: str, topic_keywords: dict[str, list[str]]) -> dict[str, list[str]]:
    tagged: dict[str, list[str]] = {}
    for topic, kws in topic_keywords.items():
        hits = sorted(set(matches(text, kws)))
        if hits:
            tagged[topic] = hits
    return tagged


def source_section(url: str) -> str:
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    host = parsed.netloc.replace("www.", "")
    if host == "bbc.com":
        return parts[0] if parts else ""
    if host == "theguardian.com":
        return parts[0] if parts else ""
    if host == "aljazeera.com":
        return parts[0] if parts else ""
    if host == "euronews.com":
        return "news"
    if host == "france24.com":
        return parts[1] if len(parts) > 1 and parts[0] == "en" else (parts[0] if parts else "")
    return parts[0] if parts else ""


def extract_meta_date(soup: BeautifulSoup) -> str | None:
    keys = [
        {"property": "article:published_time"},
        {"name": "article:published_time"},
        {"name": "pubdate"},
        {"name": "date"},
        {"name": "dc.date"},
    ]
    for attrs in keys:
        tag = soup.find("meta", attrs=attrs)
        if tag and tag.get("content"):
            return tag["content"]
    t = soup.find("time")
    if t:
        return t.get("datetime") or t.get_text(" ", strip=True)
    return None


def extract_article(url: str, html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    title = None
    for sel in ["h1", "meta[property='og:title']", "title"]:
        tag = soup.select_one(sel)
        if tag:
            title = tag.get("content") if tag.name == "meta" else tag.get_text(" ", strip=True)
            if title:
                break
    text = ""
    if trafilatura:
        text = trafilatura.extract(html, url=url, include_comments=False, include_tables=False) or ""
    if not text:
        paras = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
        text = "\n".join(p for p in paras if len(p) > 40)
    published_raw = extract_meta_date(soup)
    published_at = published_raw
    if published_raw and parse_date:
        try:
            published_at = parse_date(published_raw).isoformat()
        except Exception:
            pass
    return {"title": title or "", "published_at": published_at, "text": text.strip()}


def load_done(path: Path) -> set[str]:
    done = set()
    if not path.exists():
        return done
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line)
                if row.get("url"):
                    done.add(row["url"])
            except Exception:
                continue
    return done


def _csv_or_list(value, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, list):
        return ",".join(str(x) for x in value)
    return str(value)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="signal-mind/config/news_loader.yaml", help="YAML runtime config")
    ap.add_argument("--from", dest="from_date", default=None)
    ap.add_argument("--to", dest="to_date", default=None)
    ap.add_argument("--keywords", default=None, help="extra comma-separated keywords")
    ap.add_argument("--keywords-file", default=None, help="YAML topic dictionary")
    ap.add_argument("--topics", default=None, help="comma-separated topic names from keywords file, or 'all'")
    ap.add_argument("--sources", default=None)
    ap.add_argument("--match-scope", choices=["archive", "article", "both"], default=None,
                    help="archive=filter by archive title/summary before download; article=download every archive link then match full text; both=archive prefilter OR full-text match")
    ap.add_argument("--delay", type=float, default=None, help="base delay between article requests")
    ap.add_argument("--archive-delay", type=float, default=None)
    ap.add_argument("--max-articles", type=int, default=None, help="0 = unlimited")
    ap.add_argument("--save-policy", choices=["matched", "all"], default=None, help="matched=save only keyword matches; all=save all scanned articles with tags")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    cfg = {}
    if args.config:
        if yaml is None:
            raise RuntimeError("PyYAML is required for --config")
        cfg_path = Path(args.config)
        if cfg_path.exists():
            cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}

    from_date = args.from_date or cfg.get("date_from")
    to_date = args.to_date or cfg.get("date_to")
    out_path = args.out or cfg.get("out")
    if not from_date or not to_date or not out_path:
        raise SystemExit("Missing date_from/date_to/out. Set them in --config or pass --from --to --out.")

    keywords_file = args.keywords_file or cfg.get("keywords_file") or "signal-mind/config/news_keywords.yaml"
    topics = args.topics or _csv_or_list(cfg.get("topics"), "all")
    sources_arg = args.sources or _csv_or_list(cfg.get("sources"), "bbc,guardian,fox")
    args.match_scope = args.match_scope or cfg.get("match_scope") or "archive"
    args.delay = args.delay if args.delay is not None else float(cfg.get("delay", 8.0))
    args.archive_delay = args.archive_delay if args.archive_delay is not None else float(cfg.get("archive_delay", 2.0))
    args.max_articles = args.max_articles if args.max_articles is not None else int(cfg.get("max_articles", 0))
    args.save_policy = args.save_policy or cfg.get("save_policy") or "matched"

    start = dt.date.fromisoformat(str(from_date))
    end = dt.date.fromisoformat(str(to_date))
    topic_keywords = load_keyword_topics(keywords_file, topics)
    keywords = flatten_keywords(topic_keywords, args.keywords)
    if not keywords:
        raise SystemExit("No keywords loaded. Use --keywords and/or --keywords-file.")
    sources = [s.strip() for s in sources_arg.split(",") if s.strip()]
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    done = load_done(out)
    written = 0

    with out.open("a", encoding="utf-8") as f:
        for day in daterange(start, end):
            for source in sources:
                au = archive_url(source, day)
                print(f"[archive] {source} {day} {au}", flush=True)
                try:
                    html = fetch(au)
                    links = parse_archive(source, day, html, au)
                except Exception as e:
                    print(f"[archive-error] {source} {day}: {e}", file=sys.stderr, flush=True)
                    continue
                if args.match_scope == "archive":
                    candidates = [x for x in links if matches(f"{x.title}\n{x.summary}", keywords)]
                    print(f"[candidates] {source} {day}: {len(candidates)} / {len(links)} archive-matched", flush=True)
                else:
                    candidates = links
                    archive_hits = sum(1 for x in links if matches(f"{x.title}\n{x.summary}", keywords))
                    print(f"[candidates] {source} {day}: {len(candidates)} / {len(links)} full-text scan; archive_hits={archive_hits}", flush=True)
                time.sleep(args.archive_delay + random.random())
                for item in candidates:
                    if item.url in done:
                        continue
                    print(f"[article] {item.url}", flush=True)
                    try:
                        article_html = fetch(item.url)
                        article = extract_article(item.url, article_html)
                        archive_text = f"{item.title}\n{item.summary}"
                        article_text = f"{article.get('title','')}\n{article.get('text','')}"
                        archive_matched = sorted(set(matches(archive_text, keywords)))
                        article_matched = sorted(set(matches(article_text, keywords)))
                        matched = sorted(set(archive_matched + article_matched))
                        topic_hits = matched_topics(f"{archive_text}\n{article_text}", topic_keywords)
                        if args.save_policy == "matched" and args.match_scope in {"article", "both"} and not matched:
                            print(f"[no-match] {item.url}", flush=True)
                            done.add(item.url)
                            continue
                        row = {
                            "id": hashlib.sha256(item.url.encode()).hexdigest()[:16],
                            "archive_date": item.date,
                            "source": item.source,
                            "source_section": source_section(item.url),
                            "url": item.url,
                            "archive_title": item.title,
                            "title": article.get("title") or item.title,
                            "published_at": article.get("published_at"),
                            "matched_keywords": matched,
                            "matched_topics": topic_hits,
                            "archive_matched_keywords": archive_matched,
                            "article_matched_keywords": article_matched,
                            "text": article.get("text", ""),
                        }
                        if row["text"]:
                            f.write(json.dumps(row, ensure_ascii=False) + "\n")
                            f.flush()
                            done.add(item.url)
                            written += 1
                        else:
                            print(f"[empty-text] {item.url}", file=sys.stderr, flush=True)
                    except Exception as e:
                        print(f"[article-error] {item.url}: {e}", file=sys.stderr, flush=True)
                    if args.max_articles and written >= args.max_articles:
                        print(f"[done] max articles reached: {written}", flush=True)
                        return 0
                    time.sleep(args.delay + random.uniform(0, args.delay * 0.5))
    print(f"[done] written={written} out={out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
