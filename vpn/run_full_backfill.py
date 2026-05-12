"""
Full EN news backfill 2025-09-01 → today through the SSH SOCKS5 tunnel.

Launches en_news_archive_loader.py with --vpn and all 6 sources, parses
its log in real time, and prints friendly status ticks on stdout:

    [21:09:29] day 2025-09-01 (1/254, 0.4%) — saved=0, ETA —
    [21:13:00] day 2025-09-02 (2/254, 0.8%) — saved=84, ETA 17h 41m
    [21:18:12] working… idle 5m, last day=2025-09-02, saved=84

Raw loader log goes to db/_en_full_run.log. The script stays in foreground;
on Ctrl-C the loader is terminated cleanly.

Usage (foreground, terminal stays open):
    .venv/Scripts/python -m vpn.run_full_backfill

Background-detached (survives terminal close):
    Start-Process .venv\\Scripts\\python.exe `
        -ArgumentList "-m vpn.run_full_backfill" `
        -RedirectStandardOutput db\\_en_full.out `
        -RedirectStandardError  db\\_en_full.err `
        -WindowStyle Hidden
    Get-Content -Wait db\\_en_full.out   # to follow progress
"""
from __future__ import annotations

import argparse
import datetime as dt
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

# Force UTF-8 on stdout/stderr so unicode arrows/dashes don't crash on Windows cp1251.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

ROOT = Path(__file__).resolve().parents[1]
LOG_PATH = ROOT / "db" / "_en_full_run.log"

DEFAULT_SOURCES = "bbc,guardian,fox,aljazeera,euronews,france24"
HEARTBEAT_IDLE_SEC = 300  # heartbeat when no parsed event for this long


def fmt_dur(seconds: float) -> str:
    s = int(seconds)
    h, s = divmod(s, 3600)
    m, _ = divmod(s, 60)
    return f"{h}h {m:02d}m" if h else f"{m}m"


def fmt_count(n: int) -> str:
    return f"{n/1000:.1f}k" if n >= 1000 else str(n)


def ts_now() -> str:
    return dt.datetime.now().strftime("%H:%M:%S")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    p.add_argument("--from", dest="date_from", default="2025-09-01")
    p.add_argument("--to", dest="date_to", default="today")
    p.add_argument("--sources", default=DEFAULT_SOURCES)
    p.add_argument("--run-id", default="en_archive_full_v1")
    p.add_argument("--db", default=None, help="override target db (default db/hf_news.db)")
    p.add_argument("--delay", type=float, default=2.0, help="article fetch delay")
    p.add_argument("--archive-delay", type=float, default=1.0, help="archive page delay")
    a = p.parse_args()

    today = dt.date.today()
    start = dt.date.fromisoformat(a.date_from)
    end = today if a.date_to == "today" else dt.date.fromisoformat(a.date_to)
    if end < start:
        sys.exit("--to must be >= --from")
    total_days = (end - start).days + 1

    target_db = a.db or str(ROOT / "db" / "hf_news.db")

    print("=" * 60)
    print(" EN news backfill")
    print("=" * 60)
    print(f" range  : {start} → {end} ({total_days} days)")
    print(f" sources: {a.sources}")
    print(f" run_id : {a.run_id}")
    print(f" target : {target_db}")
    print(f" delays : article={a.delay}s  archive={a.archive_delay}s")
    print(f" log    : {LOG_PATH}")
    print(f" started: {dt.datetime.now().isoformat(timespec='seconds')}")
    print("=" * 60)
    print()

    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable, "-u",
        str(ROOT / "en_news_archive_loader.py"),
        "--from", a.date_from,
        "--to", a.date_to,
        "--sources", a.sources,
        "--vpn", "--commit",
        "--run-id", a.run_id,
        "--delay", str(a.delay),
        "--archive-delay", str(a.archive_delay),
    ]
    if a.db:
        cmd += ["--db", a.db]

    started_at = time.time()
    state = {
        "day": None,
        "saved": 0,
        "candidates": 0,
        "dups": 0,
        "last_event": started_at,
    }
    state_lock = threading.Lock()
    stop_flag = threading.Event()

    def heartbeat_loop() -> None:
        while not stop_flag.wait(30):
            with state_lock:
                idle = time.time() - state["last_event"]
                if idle >= HEARTBEAT_IDLE_SEC:
                    print(
                        f"[{ts_now()}] working… idle {fmt_dur(idle)}, "
                        f"last day={state['day']}, saved={fmt_count(state['saved'])}",
                        flush=True,
                    )
                    # Avoid spamming: reset clock so we wait another full idle window.
                    state["last_event"] = time.time()

    hb = threading.Thread(target=heartbeat_loop, daemon=True)
    hb.start()

    re_day = re.compile(r"=== (\d{4}-\d{2}-\d{2}) ===")
    re_progress = re.compile(r"progress: saved=(\d+) candidates=(\d+) dups=(\d+)")
    re_candidates = re.compile(r"(\w+) (\d{4}-\d{2}-\d{2}): (\d+) candidates / (\d+) archive links")
    re_archive_err = re.compile(r"archive error|ConnectionReset|TimeoutError|Forbidden", re.IGNORECASE)

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )

    try:
        with LOG_PATH.open("w", encoding="utf-8") as log_f:
            assert proc.stdout is not None
            for line in proc.stdout:
                log_f.write(line)
                log_f.flush()

                m = re_day.search(line)
                if m:
                    day = dt.date.fromisoformat(m.group(1))
                    elapsed = time.time() - started_at
                    done = (day - start).days
                    pct = (done / total_days) * 100 if total_days else 0
                    eta_s = (
                        fmt_dur(elapsed / done * (total_days - done))
                        if done > 0 else "—"
                    )
                    with state_lock:
                        state["day"] = day
                        state["last_event"] = time.time()
                    print(
                        f"[{ts_now()}] day {day} "
                        f"({done + 1}/{total_days}, {pct:.1f}%) — "
                        f"saved={fmt_count(state['saved'])}, "
                        f"dups={fmt_count(state['dups'])}, "
                        f"ETA {eta_s}",
                        flush=True,
                    )
                    continue

                m = re_progress.search(line)
                if m:
                    with state_lock:
                        state["saved"] = int(m.group(1))
                        state["candidates"] = int(m.group(2))
                        state["dups"] = int(m.group(3))
                        state["last_event"] = time.time()
                    continue

                if re_candidates.search(line):
                    with state_lock:
                        state["last_event"] = time.time()
                    continue

                # surface real errors to terminal
                if re_archive_err.search(line):
                    sys.stdout.write(f"[{ts_now()}] ! {line.strip()}\n")
                    sys.stdout.flush()
    except KeyboardInterrupt:
        print(f"\n[{ts_now()}] Ctrl-C — terminating loader…", flush=True)
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        stop_flag.set()
        return 130
    finally:
        stop_flag.set()

    rc = proc.wait()
    elapsed = time.time() - started_at
    print()
    print("=" * 60)
    print(f" DONE in {fmt_dur(elapsed)} — exit={rc}")
    print(f" saved : {state['saved']}")
    print(f" dups  : {state['dups']}")
    print(f" log   : {LOG_PATH}")
    print("=" * 60)
    return rc


if __name__ == "__main__":
    sys.exit(main())
