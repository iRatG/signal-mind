"""Snapshot progress of an in-flight ru_news_archive_loader run.

Default RUN_ID = ru_archive_full_v1.

Reports:
  - run status (running / done / error / interrupted)
  - started_at, finished_at (if any), elapsed
  - articles inserted so far, by source
  - per-day progress (last 5 days processed + total days covered)
  - speed: articles/min, days/hour, ETA for full range
  - last 10 lines containing WARNING/ERROR from the run log
  - tail of stats_json from archive_load_runs

Run: .venv/Scripts/python check_progress.py
     .venv/Scripts/python check_progress.py ru_archive_test_week_v1   # other run_id
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent
DB = ROOT / "db" / "hf_news.db"
DEFAULT_RUN_ID = "ru_archive_full_v1"
DEFAULT_LOG = ROOT / "db" / "_archive_full_run.log"


def fmt_secs(seconds: float) -> str:
    if seconds < 0:
        return "?"
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def main() -> int:
    run_id = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_RUN_ID
    log_path = Path(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_LOG

    if not DB.exists():
        print(f"ERROR: {DB} does not exist")
        return 1

    con = sqlite3.connect(str(DB))
    con.row_factory = sqlite3.Row

    # 1) Run audit row
    run = con.execute(
        "SELECT * FROM archive_load_runs WHERE run_id=?", (run_id,)
    ).fetchone()
    if not run:
        print(f"No archive_load_runs row for run_id={run_id!r}")
        print("Available run_ids:")
        for r in con.execute(
            "SELECT run_id, status, started_at FROM archive_load_runs ORDER BY started_at DESC LIMIT 10"
        ):
            print(f"  {r['run_id']:40s} {r['status']:15s} {r['started_at']}")
        con.close()
        return 1

    print(f"\n=== Run {run_id} ===")
    print(f"  status        : {run['status']}")
    print(f"  db_path       : {run['db_path']}")
    print(f"  range         : {run['date_from']} -> {run['date_to']}")
    print(f"  sources       : {run['sources']}")
    print(f"  dry_run       : {bool(run['dry_run'])}")
    print(f"  started_at    : {run['started_at']}")
    print(f"  finished_at   : {run['finished_at']}")

    # Elapsed
    try:
        started = datetime.fromisoformat(run["started_at"].replace("Z", "+00:00"))
        if run["finished_at"]:
            ended = datetime.fromisoformat(run["finished_at"].replace("Z", "+00:00"))
        else:
            ended = datetime.now(timezone.utc)
        elapsed = (ended - started).total_seconds()
        print(f"  elapsed       : {fmt_secs(elapsed)}")
    except Exception:
        elapsed = 0

    # 2) Inserted articles by source
    rows = con.execute(
        "SELECT source, COUNT(*) c FROM article_dedup WHERE run_id=? GROUP BY source",
        (run_id,),
    ).fetchall()
    total = sum(r["c"] for r in rows)
    print(f"\n  inserted      : {total}")
    for r in rows:
        print(f"    {r['source']:15s} {r['c']:6d}")

    # 3) Per-day distribution
    article_ids = [
        r["article_id"]
        for r in con.execute("SELECT article_id FROM article_dedup WHERE run_id=?", (run_id,))
    ]
    if article_ids:
        ph = ",".join("?" * len(article_ids))
        days = con.execute(
            f"SELECT date, COUNT(*) c FROM articles WHERE id IN ({ph}) GROUP BY date ORDER BY date",
            article_ids,
        ).fetchall()
        print(f"\n  days covered  : {len(days)}")
        if days:
            print(f"  first day     : {days[0]['date']}")
            print(f"  last day      : {days[-1]['date']}")
            print(f"  last 5 days:")
            for d in days[-5:]:
                print(f"    {d['date']}  {d['c']:4d}")

            # 4) Speed and ETA
            try:
                first = datetime.strptime(run["date_from"], "%Y-%m-%d").date()
                last_target = datetime.strptime(run["date_to"], "%Y-%m-%d").date()
                last_done = datetime.strptime(days[-1]["date"], "%Y-%m-%d").date()
                total_days = (last_target - first).days + 1
                done_days = (last_done - first).days + 1
                remaining = total_days - done_days

                if elapsed > 0 and done_days > 0:
                    art_per_min = total / (elapsed / 60)
                    sec_per_day = elapsed / done_days
                    eta_sec = remaining * sec_per_day
                    print(f"\n  speed         : {art_per_min:.1f} art/min, {sec_per_day/60:.1f} min/day")
                    print(f"  done          : {done_days} / {total_days} days ({done_days*100//total_days}%)")
                    print(f"  remaining     : {remaining} days, ETA {fmt_secs(eta_sec)}")
            except Exception as e:
                print(f"  (speed calc skipped: {e})")
    else:
        print("\n  (no articles yet)")

    # 5) Tail of WARNING/ERROR from log
    if log_path.exists():
        try:
            data = log_path.read_bytes()
            text = data.decode("utf-8", errors="replace")
            warns = [line for line in text.splitlines() if "WARNING" in line or "ERROR" in line or "Traceback" in line]
            print(f"\n  log warnings  : {len(warns)} lines in {log_path.name}")
            for line in warns[-10:]:
                print(f"    {line[:200]}")
        except Exception as e:
            print(f"  (log read error: {e})")
    else:
        print(f"\n  log file not found: {log_path}")

    # 6) Stats from run row
    if run["stats_json"]:
        try:
            stats = json.loads(run["stats_json"])
            errs = stats.get("errors", [])
            print(f"\n  stats.candidates  : {stats.get('candidates')}")
            print(f"  stats.parsed_valid: {stats.get('parsed_valid')}")
            print(f"  stats.inserted    : {stats.get('inserted')}")
            print(f"  stats.duplicates  : {stats.get('duplicates')}")
            print(f"  stats.errors      : {len(errs)}")
            if errs:
                print("  recent errors:")
                for e in errs[-5:]:
                    print(f"    - {e[:200]}")
        except Exception:
            pass

    con.close()
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
