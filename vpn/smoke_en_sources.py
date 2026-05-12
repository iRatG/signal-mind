"""
Smoke test: parse 1 day across all 6 EN sources through the VPN tunnel.

Dry-run (does NOT write to db/hf_news.db). Uses the loader's own
primitives (archive_url, parse_archive, extract_article, find_matches)
so it stays in sync with the real pipeline.

Usage:
    .venv/Scripts/python -m vpn.smoke_en_sources
    .venv/Scripts/python -m vpn.smoke_en_sources --day 2025-09-01
    .venv/Scripts/python -m vpn.smoke_en_sources --no-vpn   # for comparison
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, datetime
from pathlib import Path

# Force UTF-8 on stdout so non-ASCII titles (curly quotes, em-dashes, etc.)
# don't crash on the Windows default cp1251 codepage.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

# Make the project root importable.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import en_news_archive_loader as L  # noqa: E402
from src.utils.proxy import external_ip, start_vpn  # noqa: E402

SOURCES = ["bbc", "guardian", "fox", "aljazeera", "euronews", "france24"]
DEFAULT_KEYWORDS = ROOT / "signal_mind_news_loader_source_only" / "config" / "news_keywords.yaml"


def smoke(day: date, use_vpn: bool, cap_matched: int = 3, cap_attempts: int = 9) -> list[dict]:
    logging.getLogger().setLevel(logging.WARNING)

    if use_vpn:
        t0 = time.time()
        start_vpn()
        tunnel_ip = external_ip(through_vpn=True)
        print(f"VPN up in {time.time()-t0:.2f}s, tunnel IP = {tunnel_ip}")
    else:
        print("VPN OFF (direct connection)")
    print()

    topic_keywords = L.load_topic_keywords(DEFAULT_KEYWORDS, "all")
    keywords = L.flatten_keywords(topic_keywords)
    print(f"Keywords: {len(keywords)} across {len(topic_keywords)} topics; day = {day}")
    print()

    client = L.HttpClient(sleep_seconds=1.0, archive_sleep=1.0, use_vpn=use_vpn)
    rows: list[dict] = []

    for s in SOURCES:
        r: dict = {
            "source": s, "t_archive": 0.0, "t_total": 0.0,
            "links": 0, "attempts": 0, "downloaded": 0, "matched": 0,
            "rej_date": 0, "rej_short": 0, "rej_no_match": 0, "errors": 0,
            "samples": [], "topics": set(), "error_msg": None,
        }
        t_start = time.time()
        try:
            au = L.archive_url(s, day)
            ta = time.time()
            html = client.get(au, jitter_base=0)
            r["t_archive"] = time.time() - ta
            links = L.parse_archive(s, day, html, au)
            r["links"] = len(links)
        except Exception as e:
            r["error_msg"] = f"archive {type(e).__name__}: {e}"
            r["errors"] = 1
            r["t_total"] = time.time() - t_start
            rows.append(r)
            print(f"  {s:<10} archive FAIL — {r['error_msg'][:120]}")
            continue

        for link in links[:cap_attempts]:
            if r["matched"] >= cap_matched:
                break
            r["attempts"] += 1
            try:
                ah = client.get(link.url, jitter_base=0)
                title, body, pub = L.extract_article(link.url, ah, day)
                r["downloaded"] += 1
            except Exception:
                r["errors"] += 1
                continue
            if pub is None or pub.date() != day:
                r["rej_date"] += 1
                continue
            if not title:
                title = link.title
            if len(body) < 120:
                r["rej_short"] += 1
                continue
            matched = sorted(set(L.find_matches(f"{title}\n{body}", keywords)))
            if not matched:
                r["rej_no_match"] += 1
                continue
            topics = L.find_matched_topics(f"{title}\n{body}", topic_keywords)
            r["topics"].update(topics)
            r["matched"] += 1
            if len(r["samples"]) < 3:
                r["samples"].append({
                    "title": title[:90],
                    "matched": matched[:5],
                    "n_matched": len(matched),
                    "len_body": len(body),
                })
        r["t_total"] = time.time() - t_start
        rows.append(r)
        print(
            f"  {s:<10} arch={r['t_archive']:5.1f}s "
            f"tot={r['t_total']:5.1f}s "
            f"links={r['links']:>3} "
            f"DL={r['downloaded']:>2}/{r['attempts']:<2} "
            f"matched={r['matched']:>2} "
            f"rej(date/short/nomatch)={r['rej_date']}/{r['rej_short']}/{r['rej_no_match']} "
            f"err={r['errors']}"
        )

    return rows


def print_report(rows: list[dict]) -> None:
    print()
    print("=" * 100)
    print("PER-SOURCE SUMMARY")
    print("=" * 100)
    hdr = f"{'source':<11} {'arch':>5} {'total':>6} {'links':>5} {'DL':>3} {'mat':>3} {'r_d':>3} {'r_s':>3} {'r_n':>3} {'err':>3}"
    print(hdr)
    print("-" * len(hdr))
    totals = {"links": 0, "downloaded": 0, "matched": 0, "rej_date": 0, "rej_short": 0, "rej_no_match": 0, "errors": 0, "t_total": 0.0}
    for r in rows:
        print(f"{r['source']:<11} "
              f"{r['t_archive']:5.1f} {r['t_total']:6.1f} "
              f"{r['links']:>5} {r['downloaded']:>3} {r['matched']:>3} "
              f"{r['rej_date']:>3} {r['rej_short']:>3} {r['rej_no_match']:>3} {r['errors']:>3}")
        for k in totals:
            totals[k] += r.get(k, 0)
    print("-" * len(hdr))
    print(f"{'TOTAL':<11} {'':>5} {totals['t_total']:6.1f} "
          f"{totals['links']:>5} {totals['downloaded']:>3} {totals['matched']:>3} "
          f"{totals['rej_date']:>3} {totals['rej_short']:>3} {totals['rej_no_match']:>3} {totals['errors']:>3}")

    print()
    print("=" * 100)
    print("SAMPLE MATCHED ARTICLES")
    print("=" * 100)
    for r in rows:
        if r["error_msg"]:
            print(f"\n--- {r['source']} ---  ERROR: {r['error_msg']}")
            continue
        topics = sorted(r["topics"]) if r["topics"] else []
        topic_str = ", ".join(topics) if topics else "(none)"
        print(f"\n--- {r['source']} ---  topics: {topic_str}")
        if not r["samples"]:
            print(f"  (no matched articles within {r['attempts']} attempts)")
            continue
        for i, sm in enumerate(r["samples"], 1):
            print(f"  [{i}] {sm['title']}")
            print(f"      matched={sm['n_matched']} kw, sample={sm['matched']}, body={sm['len_body']} chars")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--day", default="2025-09-01", help="YYYY-MM-DD")
    p.add_argument("--no-vpn", action="store_true", help="Skip VPN, use direct connection")
    p.add_argument("--cap", type=int, default=3, help="matched articles per source")
    p.add_argument("--attempts", type=int, default=9, help="article-download attempts per source")
    a = p.parse_args()

    day = datetime.strptime(a.day, "%Y-%m-%d").date()
    rows = smoke(day, use_vpn=not a.no_vpn, cap_matched=a.cap, cap_attempts=a.attempts)
    print_report(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
