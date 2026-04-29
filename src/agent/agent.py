"""Signal Mind — main agent loop (Phase 3).

Ouroboros cycle: generate hypothesis → verify with SQL → evaluate → feed result
back as context for the next hypothesis. Each iteration builds on the previous.
"""
import json
import time
from datetime import datetime
from pathlib import Path

from src.agent.hypothesis import run_hypothesis_cycle

LOG_DIR = Path(__file__).parents[2] / "db"
SIGNALS_FILE = LOG_DIR / "signals.jsonl"


def save_signal(result: dict):
    LOG_DIR.mkdir(exist_ok=True)
    record = {
        "ts": datetime.now().isoformat(),
        "hypothesis": result.get("hypothesis"),
        "confirmed": result.get("confirmed"),
        "signal_score": result.get("signal_score"),
        "finding": result.get("finding"),
        "signal_brief": result.get("signal_brief"),
        "next_hypothesis": result.get("next_hypothesis"),
        "sql": result.get("sql"),
    }
    with open(SIGNALS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def print_result(result: dict, iteration: int):
    score = result.get("signal_score", 0)
    confirmed = result.get("confirmed")
    status = "CONFIRMED" if confirmed else ("PARTIAL" if confirmed is None else "REJECTED")

    print(f"\n{'='*60}")
    print(f"ITERATION {iteration} | Score: {score}/100 | {status}")
    print(f"{'='*60}")
    print(f"Hypothesis : {result.get('hypothesis', '?')}")
    print(f"Finding    : {result.get('finding', '?')}")
    print(f"Signal     : {result.get('signal_brief', '?')}")
    print(f"Next       : {result.get('next_hypothesis', '?')}")


def run(iterations: int = 3, start_hint: str = ""):
    """Run the Ouroboros cycle for N iterations."""
    print(f"Signal Mind Agent starting — {iterations} iterations")
    print(f"Signals will be saved to: {SIGNALS_FILE}")

    context = start_hint
    for i in range(1, iterations + 1):
        print(f"\n--- Iteration {i}/{iterations} ---")
        try:
            result = run_hypothesis_cycle(context)
            save_signal(result)
            print_result(result, i)
            # Feed next_hypothesis as context for the next iteration
            context = result.get("next_hypothesis", "")
            time.sleep(2)
        except Exception as e:
            print(f"Error in iteration {i}: {e}")
            continue

    print(f"\nDone. Signals saved to {SIGNALS_FILE}")
    _print_summary()


def _print_summary():
    if not SIGNALS_FILE.exists():
        return
    signals = [json.loads(l) for l in SIGNALS_FILE.read_text(encoding="utf-8").strip().splitlines()]
    if not signals:
        return
    print(f"\n{'='*60}")
    print(f"SESSION SUMMARY — {len(signals)} signals total")
    print(f"{'='*60}")
    for s in signals[-5:]:
        score = s.get("signal_score", 0)
        conf = "Y" if s.get("confirmed") else ("?" if s.get("confirmed") is None else "N")
        print(f"  [{conf}] {score:>3}/100  {s.get('hypothesis', '')[:65]}")


if __name__ == "__main__":
    import sys
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    run(iterations=n)
