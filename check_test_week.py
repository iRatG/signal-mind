"""Inspect quality of ru_archive_test_week_v1 run in db/hf_news.db.

Verifies:
  - Total articles loaded under this run_id
  - Per-source / per-day distribution
  - Body length distribution (min, p25, p50, p75, max, mean)
  - All `date` values are inside 2025-10-01..2025-10-07
  - No URL-level duplicates within this run
  - Encoding sanity (cyrillic preserved in titles/bodies)
  - 5 random samples (title + 200 chars of body) for eyeball

Run: .venv/Scripts/python check_test_week.py
"""
from __future__ import annotations

import json
import random
import sqlite3
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

DB = Path(__file__).resolve().parent / "db" / "hf_news.db"
RUN_ID = sys.argv[1] if len(sys.argv) > 1 else "ru_archive_test_week_v1"
EXPECTED_FROM = sys.argv[2] if len(sys.argv) > 2 else "2025-10-01"
EXPECTED_TO = sys.argv[3] if len(sys.argv) > 3 else "2025-10-07"


def percentile(values: list[int], p: float) -> int:
    if not values:
        return 0
    s = sorted(values)
    k = int(round((len(s) - 1) * p))
    return s[k]


def main() -> int:
    if not DB.exists():
        print(f"ERROR: {DB} does not exist")
        return 1

    con = sqlite3.connect(str(DB))
    con.row_factory = sqlite3.Row

    # 1) Find article ids belonging to this run
    rows = con.execute(
        "SELECT article_id, url, source FROM article_dedup WHERE run_id=?",
        (RUN_ID,),
    ).fetchall()
    article_ids = [r["article_id"] for r in rows]
    print(f"\n=== Run {RUN_ID} ===")
    print(f"  article_dedup rows : {len(rows)}")
    if not article_ids:
        print("  (no articles under this run_id — nothing to inspect)")
        con.close()
        return 0

    # 2) Pull articles
    placeholders = ",".join("?" * len(article_ids))
    arts = con.execute(
        f"SELECT id, source, date, text, extra_fields, loaded_at FROM articles WHERE id IN ({placeholders})",
        article_ids,
    ).fetchall()
    print(f"  articles rows      : {len(arts)}")

    # 3) Per-source / per-day distribution
    by_source: dict[str, int] = {}
    by_date: dict[str, int] = {}
    for a in arts:
        by_source[a["source"]] = by_source.get(a["source"], 0) + 1
        by_date[a["date"]] = by_date.get(a["date"], 0) + 1

    print("\n  By source:")
    for s in sorted(by_source):
        print(f"    {s:30s} {by_source[s]:6d}")

    print("\n  By date:")
    for d in sorted(by_date):
        print(f"    {d}  {by_date[d]:6d}")

    # 4) Date sanity
    out_of_range = [a["date"] for a in arts if a["date"] < EXPECTED_FROM or a["date"] > EXPECTED_TO]
    print(f"\n  Dates out of {EXPECTED_FROM}..{EXPECTED_TO} : {len(out_of_range)}")
    if out_of_range:
        print(f"    Examples: {out_of_range[:5]}")

    # 5) Body length distribution
    lens = [len(a["text"]) for a in arts]
    if lens:
        print("\n  Body+title text length (chars):")
        print(f"    min     : {min(lens):8d}")
        print(f"    p25     : {percentile(lens, 0.25):8d}")
        print(f"    median  : {percentile(lens, 0.50):8d}")
        print(f"    p75     : {percentile(lens, 0.75):8d}")
        print(f"    max     : {max(lens):8d}")
        print(f"    mean    : {sum(lens) // len(lens):8d}")
        print(f"    < 200   : {sum(1 for x in lens if x < 200)}")
        print(f"    > 10000 : {sum(1 for x in lens if x > 10000)}")

    # 6) Encoding sanity — count cyrillic-containing rows
    cyr = sum(1 for a in arts if any("Ѐ" <= ch <= "ӿ" for ch in a["text"][:200]))
    print(f"\n  Articles with cyrillic in first 200 chars : {cyr} / {len(arts)}")

    # 7) URL duplicates within run
    url_seen: dict[str, int] = {}
    for r in rows:
        url_seen[r["url"]] = url_seen.get(r["url"], 0) + 1
    dups = {u: c for u, c in url_seen.items() if c > 1}
    print(f"\n  URL duplicates within run : {len(dups)}")
    if dups:
        for u, c in list(dups.items())[:5]:
            print(f"    {c}x  {u}")

    # 8) Run summary from archive_load_runs
    run_row = con.execute(
        "SELECT status, started_at, finished_at, dry_run, stats_json FROM archive_load_runs WHERE run_id=?",
        (RUN_ID,),
    ).fetchone()
    if run_row:
        print(f"\n  Run status : {run_row['status']}")
        print(f"  Started    : {run_row['started_at']}")
        print(f"  Finished   : {run_row['finished_at']}")
        try:
            stats = json.loads(run_row["stats_json"])
            print(f"  Stats.parsed_valid : {stats.get('parsed_valid')}")
            print(f"  Stats.inserted     : {stats.get('inserted')}")
            print(f"  Stats.duplicates   : {stats.get('duplicates')}")
            print(f"  Stats.errors       : {len(stats.get('errors', []))}")
            errs = stats.get("errors", [])
            if errs:
                print("  First 3 errors:")
                for e in errs[:3]:
                    print(f"    - {e}")
        except Exception as e:
            print(f"  (stats_json parse error: {e})")

    # 9) Random sample of 5 articles
    print("\n  ----- Random sample of 5 articles -----")
    sample = random.sample(arts, min(5, len(arts)))
    for i, a in enumerate(sample, 1):
        try:
            extra = json.loads(a["extra_fields"])
            title = extra.get("title", "")[:120]
            url = extra.get("url", "")
        except Exception:
            title, url = "", ""
        body_preview = a["text"][:200].replace("\n", " ")
        print(f"\n  [{i}] {a['source']}  date={a['date']}  id={a['id']}")
        print(f"      url   : {url}")
        print(f"      title : {title}")
        print(f"      body  : {body_preview}...")

    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
