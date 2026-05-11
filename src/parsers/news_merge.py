"""news_merge.py — append gap news counts into news_daily (DuckDB).

Sources:
  1. db/news_gdelt.db    — GDELT DOC API daily counts (English, pivoted by topic)
  2. db/news_archive.db  — cc_news Russian articles (raw, keyword-classified)

Rules:
  - Only INSERT dates not already in news_daily (never overwrite hf_news.db data)
  - Counts from both sources are summed per (date, topic)
  - news_archive.db articles are re-classified to our 7 financial topics

Usage:
    python -m src.parsers.news_merge
    python -m src.parsers.news_merge --from 2025-09-07 --to 2026-05-02
    python -m src.parsers.news_merge --dry-run
"""
from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import duckdb

DUCK_PATH   = Path(__file__).parents[2] / "db" / "signal_mind.duckdb"
GDELT_PATH  = Path(__file__).parents[2] / "db" / "news_gdelt.db"
ARCHIVE_PATH = Path(__file__).parents[2] / "db" / "news_archive.db"

TOPICS = ["oil", "rate", "ruble", "sanctions", "inflation", "banking", "gold"]

# Russian article keywords → financial topic mapping
# Matches against article title + matched_kw field (case-insensitive)
RU_TOPIC_KW: dict[str, list[str]] = {
    "oil":       ["нефть", "brent", "urals", "газ", "нефтегазов", "энергетик"],
    "rate":      ["ключевая ставка", "цб", "центробанк", "процентная ставка", "кбр"],
    "ruble":     ["рубль", "курс рубля", "девальвация", "укрепление рубля"],
    "sanctions": ["санкции", "контрсанкции", "эмбарго", "ограничения"],
    "inflation": ["инфляция", "ипц", "потребительские цены", "cpi"],
    "banking":   ["банк", "банковск", "кредит", "сбер", "втб"],
    "gold":      ["золото", "драгметалл", "rugold", "драгоценн"],
}


# ── helpers ───────────────────────────────────────────────────────────────────

def _existing_dates(duck: duckdb.DuckDBPyConnection) -> set[str]:
    rows = duck.execute("SELECT news_date::TEXT FROM news_daily").fetchall()
    return {r[0] for r in rows}


def _parse_date(val: str | None) -> str | None:
    """Normalize various date strings to YYYY-MM-DD or None."""
    if not val:
        return None
    val = str(val).strip()
    # datetime string: "2025-09-10 ..." or "2025-09-10T..."
    if len(val) >= 10 and val[4] == "-":
        return val[:10]
    # YYYYMMDD
    if len(val) == 8 and val.isdigit():
        return f"{val[:4]}-{val[4:6]}-{val[6:8]}"
    return None


# ── source readers ────────────────────────────────────────────────────────────

def _read_gdelt(start: date, end: date, skip_dates: set[str]) -> dict[str, dict[str, int]]:
    """Return {date_str: {topic: count}} from news_gdelt.db."""
    if not GDELT_PATH.exists():
        print(f"  [merge] news_gdelt.db not found — skipping GDELT source")
        return {}

    con = sqlite3.connect(str(GDELT_PATH))
    rows = con.execute(
        "SELECT news_date, topic, count FROM gdelt_daily "
        "WHERE news_date >= ? AND news_date <= ?",
        (str(start), str(end)),
    ).fetchall()
    con.close()

    result: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for date_str, topic, count in rows:
        if date_str in skip_dates:
            continue
        if topic in TOPICS:
            result[date_str][topic] += count

    print(f"  [merge] GDELT: {len(rows)} long-rows → {len(result)} dates")
    return result


def _read_ccnews(start: date, end: date, skip_dates: set[str]) -> dict[str, dict[str, int]]:
    """Return {date_str: {topic: count}} from news_archive.db (Russian articles)."""
    if not ARCHIVE_PATH.exists():
        return {}

    con = sqlite3.connect(str(ARCHIVE_PATH))
    count_total = con.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    if count_total == 0:
        con.close()
        print("  [merge] news_archive.db is empty — skipping cc_news source")
        return {}

    rows = con.execute(
        "SELECT title, matched_kw, date FROM articles "
        "WHERE date >= ? AND date <= ?",
        (str(start).replace("-", ""), str(end).replace("-", "")),
    ).fetchall()
    con.close()

    result: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for title, matched_kw, raw_date in rows:
        date_str = _parse_date(raw_date)
        if not date_str or date_str in skip_dates:
            continue

        hay = ((title or "") + " " + (matched_kw or "")).lower()
        for topic, keywords in RU_TOPIC_KW.items():
            if any(kw in hay for kw in keywords):
                result[date_str][topic] += 1

    print(f"  [merge] cc_news: {len(rows)} articles → {len(result)} dates")
    return result


# ── merge ─────────────────────────────────────────────────────────────────────

def _combine(
    gdelt: dict[str, dict[str, int]],
    ccnews: dict[str, dict[str, int]],
) -> dict[str, dict[str, int]]:
    """Merge both sources: sum counts for same (date, topic)."""
    combined: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for date_str, topics in gdelt.items():
        for topic, count in topics.items():
            combined[date_str][topic] += count
    for date_str, topics in ccnews.items():
        for topic, count in topics.items():
            combined[date_str][topic] += count
    return combined


def run(
    start: date | None = None,
    end: date | None = None,
    dry_run: bool = False,
) -> dict:
    duck = duckdb.connect(str(DUCK_PATH))

    if end is None:
        end = date.today()
    if start is None:
        # Default: day after last loaded date in news_daily
        row = duck.execute("SELECT MAX(news_date) FROM news_daily").fetchone()
        last = row[0] if row and row[0] else date(2025, 9, 6)
        start = last + timedelta(days=1)
        if hasattr(start, 'date'):   # duck returns datetime.date already
            start = start

    print(f"[news_merge] Gap: {start} to {end}  ({(end - start).days + 1} days)")

    existing = _existing_dates(duck)
    skip = existing  # only skip dates already in news_daily

    gdelt_data  = _read_gdelt(start, end, skip)
    ccnews_data = _read_ccnews(start, end, skip)
    combined    = _combine(gdelt_data, ccnews_data)

    if not combined:
        print("[news_merge] No new data to insert.")
        duck.close()
        return {"inserted": 0}

    # Build rows to insert
    rows: list[tuple] = []
    for date_str in sorted(combined.keys()):
        if date_str in skip:
            continue
        t = combined[date_str]
        rows.append((
            date_str,
            t.get("oil", 0),
            t.get("rate", 0),
            t.get("ruble", 0),
            t.get("sanctions", 0),
            t.get("inflation", 0),
            t.get("banking", 0),
            t.get("gold", 0),
        ))

    print(f"[news_merge] Rows to insert: {len(rows)}")
    if rows:
        sample = rows[:3]
        for s in sample:
            print(f"  {s}")
        if len(rows) > 3:
            print(f"  ... and {len(rows) - 3} more")

    if dry_run:
        print("[news_merge] DRY RUN — nothing written.")
        duck.close()
        return {"inserted": 0, "would_insert": len(rows)}

    # INSERT (not REPLACE) — preserve existing hf_news.db rows
    inserted = 0
    for row in rows:
        try:
            duck.execute(
                "INSERT INTO news_daily "
                "(news_date, oil, rate, ruble, sanctions, inflation, banking, gold) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                row,
            )
            inserted += 1
        except Exception:
            pass  # skip if date already exists (race condition safety)

    # Summary
    stats = duck.execute(
        "SELECT MIN(news_date), MAX(news_date), COUNT(*) FROM news_daily"
    ).fetchone()
    print(f"\n[news_merge] Done. Inserted: {inserted} new rows.")
    print(f"[news_merge] news_daily: {stats[0]} → {stats[1]} ({stats[2]} total days)")
    duck.close()
    return {"inserted": inserted, "range_min": str(stats[0]), "range_max": str(stats[1])}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge gap news into news_daily")
    parser.add_argument("--from", dest="from_date", help="Start date YYYY-MM-DD")
    parser.add_argument("--to",   dest="to_date",   help="End date YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = parser.parse_args()

    s = date.fromisoformat(args.from_date) if args.from_date else None
    e = date.fromisoformat(args.to_date)   if args.to_date   else None
    run(s, e, dry_run=args.dry_run)
