"""GDELT DOC API → db/news_gdelt.db

Downloads daily article volume counts per financial topic for the gap period.
Requests are split into monthly chunks to avoid rate limits.
Append-safe: skips dates already loaded. No API key required.

Storage schema:
    news_gdelt.db / gdelt_daily (news_date TEXT, topic TEXT, count INTEGER)

Usage:
    python -m src.parsers.gdelt_loader                     # auto-detect gap from last loaded date
    python -m src.parsers.gdelt_loader --from 2025-09-07   # explicit start
    python -m src.parsers.gdelt_loader --from 2025-09-07 --to 2026-05-02
"""
from __future__ import annotations

import argparse
import sqlite3
import time
from datetime import date, timedelta
from pathlib import Path

import requests

DB_PATH = Path(__file__).parents[2] / "db" / "news_gdelt.db"

GAP_START = date(2025, 9, 7)   # first day not in hf_news.db

TOPIC_QUERIES: dict[str, str] = {
    "oil":       "oil price Brent crude energy petroleum",
    "rate":      "interest rate central bank monetary policy Fed ECB",
    "ruble":     "ruble Russian currency RUB exchange rate",
    "sanctions": "Russia sanctions embargo restrictions",
    "inflation": "inflation CPI consumer price index",
    "banking":   "banking sector financial bank credit",
    "gold":      "gold price precious metal XAU",
}

GDELT_API = "https://api.gdeltproject.org/api/v2/doc/doc"

SLEEP_BETWEEN_REQUESTS = 8.0   # seconds between API calls
MAX_RETRIES = 5


# ── DB ──────────────────────────────────────────────────────────────────────

def _init_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(exist_ok=True)
    con = sqlite3.connect(str(DB_PATH))
    con.execute("""
        CREATE TABLE IF NOT EXISTS gdelt_daily (
            news_date  TEXT NOT NULL,
            topic      TEXT NOT NULL,
            count      INTEGER DEFAULT 0,
            source     TEXT DEFAULT 'gdelt_api',
            loaded_at  TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (news_date, topic)
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_gd_date ON gdelt_daily(news_date)")
    con.commit()
    return con


def _loaded_dates(con: sqlite3.Connection, topic: str) -> set[str]:
    rows = con.execute(
        "SELECT news_date FROM gdelt_daily WHERE topic = ?", (topic,)
    ).fetchall()
    return {r[0] for r in rows}


def _max_loaded_date(con: sqlite3.Connection) -> date | None:
    row = con.execute("SELECT MAX(news_date) FROM gdelt_daily").fetchone()
    if row and row[0]:
        return date.fromisoformat(row[0])
    return None


# ── month iterator ──────────────────────────────────────────────────────────

def _month_chunks(start: date, end: date):
    """Yield (chunk_start, chunk_end) pairs covering [start, end] by month."""
    cur = start
    while cur <= end:
        # end of current month
        if cur.month == 12:
            month_end = date(cur.year + 1, 1, 1) - timedelta(days=1)
        else:
            month_end = date(cur.year, cur.month + 1, 1) - timedelta(days=1)
        chunk_end = min(month_end, end)
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


# ── GDELT API ────────────────────────────────────────────────────────────────

def _fetch_timeline(topic: str, query: str, start: date, end: date) -> dict[str, int]:
    """Call GDELT DOC API for one month chunk, return {YYYY-MM-DD: count}."""
    params = {
        "query":          query,
        "mode":           "timelinevolume",
        "format":         "json",
        "startdatetime":  start.strftime("%Y%m%d") + "000000",
        "enddatetime":    end.strftime("%Y%m%d") + "235959",
        "TIMELINESMOOTH": "0",
    }
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(GDELT_API, params=params, timeout=45)
            if resp.status_code == 429:
                wait = 30 * (2 ** attempt)   # 30, 60, 120, 240, 480s
                print(f"      429 rate limit, waiting {wait}s...", flush=True)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            if not resp.text.strip():
                print(f"      Empty response for {topic} {start}-{end}, skipping", flush=True)
                return {}
            data = resp.json()
            break
        except requests.exceptions.ConnectionError:
            print(f"      Connection error — no internet? Skipping {topic}")
            return {}
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait = 20 * (attempt + 1)
                print(f"      Error ({e}), retry in {wait}s...", flush=True)
                time.sleep(wait)
            else:
                print(f"      Failed after {MAX_RETRIES} attempts: {e}")
                return {}
    else:
        return {}

    results: dict[str, int] = {}
    for series in data.get("timeline", []):
        for point in series.get("data", []):
            raw = point.get("date", "")[:8]
            if len(raw) < 8:
                continue
            date_str = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
            results[date_str] = results.get(date_str, 0) + int(point.get("value", 0))
    return results


# ── main ─────────────────────────────────────────────────────────────────────

def run(start: date | None = None, end: date | None = None) -> dict:
    con = _init_db()

    if end is None:
        end = date.today()
    if start is None:
        last = _max_loaded_date(con)
        start = (last + timedelta(days=1)) if last else GAP_START

    if start > end:
        print(f"[gdelt_loader] Already up to date (last={start - timedelta(days=1)})")
        con.close()
        return {"inserted": 0}

    months = list(_month_chunks(start, end))
    print(f"[gdelt_loader] {start} to {end} — {len(months)} month chunks x {len(TOPIC_QUERIES)} topics")
    print(f"[gdelt_loader] Sleep {SLEEP_BETWEEN_REQUESTS}s between requests (~{len(months)*len(TOPIC_QUERIES)*SLEEP_BETWEEN_REQUESTS/60:.0f} min total)")

    total_new = 0
    req_count = 0

    for topic, query in TOPIC_QUERIES.items():
        loaded = _loaded_dates(con, topic)
        topic_new = 0
        print(f"  [{topic}]", flush=True)

        for chunk_start, chunk_end in months:
            # Skip if all days in this chunk already loaded
            chunk_days = {
                str(chunk_start + timedelta(days=i))
                for i in range((chunk_end - chunk_start).days + 1)
            }
            if chunk_days.issubset(loaded):
                print(f"    {chunk_start}..{chunk_end} already loaded, skip", flush=True)
                continue

            print(f"    {chunk_start}..{chunk_end} fetching...", end=" ", flush=True)
            data = _fetch_timeline(topic, query, chunk_start, chunk_end)

            inserted = 0
            for date_str, count in sorted(data.items()):
                if date_str not in loaded:
                    con.execute(
                        "INSERT OR REPLACE INTO gdelt_daily (news_date, topic, count, source) "
                        "VALUES (?,?,?,'gdelt_api')",
                        (date_str, topic, count),
                    )
                    loaded.add(date_str)
                    inserted += 1
            con.commit()
            topic_new += inserted
            req_count += 1
            print(f"got {len(data)} days, new={inserted}", flush=True)

            if req_count < len(months) * len(TOPIC_QUERIES):
                time.sleep(SLEEP_BETWEEN_REQUESTS)

        total_new += topic_new
        print(f"  [{topic}] total new={topic_new}", flush=True)

    row = con.execute(
        "SELECT MIN(news_date), MAX(news_date), COUNT(DISTINCT news_date) FROM gdelt_daily"
    ).fetchone()
    print(f"\n[gdelt_loader] Done. New rows: {total_new}")
    if row[0]:
        print(f"[gdelt_loader] DB: {row[0]} to {row[1]} ({row[2]} distinct dates)")
    con.close()
    return {"inserted": total_new, "range_min": row[0], "range_max": row[1]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load GDELT news counts for gap period")
    parser.add_argument("--from", dest="from_date", help="Start date YYYY-MM-DD")
    parser.add_argument("--to",   dest="to_date",   help="End date YYYY-MM-DD")
    args = parser.parse_args()

    s = date.fromisoformat(args.from_date) if args.from_date else None
    e = date.fromisoformat(args.to_date)   if args.to_date   else None
    run(s, e)
