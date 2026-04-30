"""Watchdog for Signal Mind marathon run.

Starts the main Ouroboros agent and monitors it every 60s.
Logs status every hour. Restarts automatically on crash.
Stops when the 9-hour wall-clock window expires.
No questions asked — fully autonomous.
"""
import subprocess
import sys
import time
import os
from datetime import datetime
from pathlib import Path

TOTAL_SECONDS = int(sys.argv[1]) if len(sys.argv) > 1 else 32400  # 9h default
LOG_FILE      = Path(__file__).parents[2] / "db" / "watchdog.log"
AGENT_LOG     = Path(__file__).parents[2] / "db" / "marathon_run.log"
CHECK_INTERVAL = 60    # check process every N seconds
HOUR_INTERVAL  = 3600  # status log every N seconds
RESTART_DELAY  = 10    # seconds to wait before restart


def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def start_agent(remaining_seconds: int) -> subprocess.Popen:
    python = Path(__file__).parents[2] / ".venv" / "Scripts" / "python.exe"
    cmd = [
        str(python), "-m", "src.agent.agent",
        "9999",   # upper-bound iterations
        "",       # no start hint — let topic pool drive
        str(max(remaining_seconds, 60)),
    ]
    log(f"Starting agent | remaining={remaining_seconds}s | cmd={' '.join(cmd)}")
    proc = subprocess.Popen(
        cmd,
        stdout=open(AGENT_LOG, "a", encoding="utf-8"),
        stderr=subprocess.STDOUT,
        cwd=str(Path(__file__).parents[2]),
    )
    log(f"Agent PID={proc.pid}")
    return proc


def main():
    t_start   = time.time()
    t_end     = t_start + TOTAL_SECONDS
    last_hour = t_start
    run_count = 0

    log(f"Watchdog started | total window={TOTAL_SECONDS}s ({TOTAL_SECONDS//3600}h) | agent_log={AGENT_LOG}")

    proc = start_agent(int(t_end - time.time()))
    run_count += 1

    while True:
        now       = time.time()
        remaining = int(t_end - now)

        # Time window expired — done
        if remaining <= 0:
            log(f"9-hour window complete. Terminating agent PID={proc.pid}.")
            try:
                proc.terminate()
            except Exception:
                pass
            break

        # Hourly status log
        if now - last_hour >= HOUR_INTERVAL:
            elapsed_h = (now - t_start) / 3600
            remain_h  = remaining / 3600
            alive     = proc.poll() is None
            log(f"Hourly check | elapsed={elapsed_h:.1f}h | remaining={remain_h:.1f}h | "
                f"agent_alive={alive} | restarts={run_count-1}")
            last_hour = now

        # Check if agent is still running
        ret = proc.poll()
        if ret is not None:
            log(f"Agent exited with code={ret}. Remaining={remaining}s. Restarting in {RESTART_DELAY}s...")
            time.sleep(RESTART_DELAY)
            remaining = int(t_end - time.time())
            if remaining <= 60:
                log(f"Less than 60s remaining — not restarting.")
                break
            proc = start_agent(remaining)
            run_count += 1

        time.sleep(CHECK_INTERVAL)

    log(f"Watchdog done. Total agent runs={run_count}.")


if __name__ == "__main__":
    main()
