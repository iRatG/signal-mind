"""Signal Mind — Ouroboros agent loop.

Three nested learning loops:
  Loop 1 (per-iteration): SQL self-repair
  Loop 2 (per-session):   Ouroboros hypothesis cycle
  Loop 3 (cross-session): Fine-tuning dataset accumulation in experiments.db
"""
import json
import random
import re
import time
import uuid
import hashlib
from datetime import datetime
from pathlib import Path

from src.agent.hypothesis import run_hypothesis_cycle
from src.agent.metrics import SessionMetrics
from src.agent.memory import (
    load_principles,
    load_knowledge,
    save_journal_entry,
    update_knowledge,
)
from src.agent.reflection import reflect, format_reflection
from src.agent import experiments
from src.agent import telemetry as tel_module

LOG_DIR       = Path(__file__).parents[2] / "db"
SIGNALS_FILE  = LOG_DIR / "signals.jsonl"
REFLECT_EVERY = 5

# Lag sweep: each iteration cycles through these values.
# 0 = pure DuckDB hypothesis (no news_daily join needed)
# >0 = lag-hypothesis: news_daily → market at T+N days
LAG_SWEEP = [0, 7, 14, 0, 30, 0, 60, 0, 90, 0]

# Topic pool for forced rotation and random injection.
# Every REFLECT_EVERY iterations context resets to next topic (cycling).
# Additionally, each non-reflection iteration has RANDOM_JUMP_PROB chance
# of jumping to a random topic instead of following the Ouroboros chain.
RANDOM_JUMP_PROB = 0.25

TOPIC_POOL = [
    # News lag → sector
    "Нефтяные новости (oil) предсказывают MOEXOG через 7/14/30 дней — проверь разные лаги",
    "Упоминания ключевой ставки (rate) предсказывают MOEXFN через 14-30 дней",
    "Санкционные новости (sanctions) и волатильность USD/RUB — режимы и лаги",
    "Инфляционные новости (inflation) как опережающий индикатор для банковского сектора",
    "Упоминания золота в новостях (gold) и цена RUGOLD/gold_usd — лаг 7-14 дней",
    "Банковские новости (banking) и динамика MOEXFN с лагом 7-30 дней",
    "Рублёвые новости (ruble) и EUR/RUB — какой лаг сильнее",
    # International markets (new instruments)
    "MSCI_WORLD и MOEXOG — влияние мировых рынков на российский нефтяной сектор",
    "MSCI_INDIA vs MOEXFN — корреляция двух развивающихся рынков",
    "FTSE_CHINA_50 и MOEXOG — Китай как драйвер нефтяного сектора России",
    "DJ_SOUTH_AFRICA и IMOEX — развивающиеся рынки в сравнении с Россией",
    "CHINA_H_SHARES и BRENT — Китайский спрос и нефть",
    "SILVER и GOLD — корреляция драгметаллов и российского рынка",
    # Pure DuckDB macro
    "DXY (индекс доллара) и USD/RUB — с лагом или без, что сильнее",
    "EUR/RUB vs USD/RUB — какой курс опережает другой, проверь LEAD/LAG",
    "Ключевая ставка ЦБ: какой сектор реагирует первым — MOEXFN или MOEXOG",
    "Brent vs MOEXOG: корреляция и лаг между мировой нефтью и акциями нефтяников",
    "Режимы рубля: поведение MOEXFN при USD/RUB > 90 vs < 70",
    "Сезонность IMOEX: какие кварталы дают аномальную доходность",
    "Реальные зарплаты (rosstat) и динамика потребительского сектора MOEX",
    "MOEX10 (голубые фишки) vs MOEXFN — кто опережает при изменении ставки",
    "Нефть Brent и курс USD/RUB: причинность или совпадение, лаги 1-30 дней",
]


BLACKLIST_PATH = LOG_DIR / "convergence_blacklist.json"
CONVERGENCE_SESSION_LIMIT = 3   # force jump after this many repeats within a session


def _load_blacklist() -> set[str]:
    """Load cross-session convergence blacklist written by Revizor."""
    try:
        data = json.loads(BLACKLIST_PATH.read_text(encoding="utf-8"))
        return set(data.get("blacklist", []))
    except Exception:
        return set()


def _fingerprint(text: str) -> str:
    """Normalize hypothesis hint to a stable fingerprint for convergence detection."""
    return re.sub(r"\d+", "N", (text or "")[:55].lower().strip())


def _principles_version(principles: str) -> str:
    return hashlib.md5(principles.encode()).hexdigest()[:8]


def _safe(text: str, width: int = 120) -> str:
    text = str(text)[:width]
    return text.encode("utf-8", errors="replace").decode("utf-8")


def save_signal(result: dict):
    LOG_DIR.mkdir(exist_ok=True)
    record = {
        "ts":             datetime.now().isoformat(),
        "hypothesis":     result.get("hypothesis"),
        "confirmed":      result.get("confirmed"),
        "signal_score":   result.get("signal_score"),
        "finding":        result.get("finding"),
        "signal_brief":   result.get("signal_brief"),
        "next_hypothesis": result.get("next_hypothesis"),
        "sql":            result.get("sql"),
    }
    with open(SIGNALS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def print_result(result: dict, iteration: int, attempts_count: int):
    score     = result.get("signal_score", 0) or 0
    confirmed = result.get("confirmed")
    status    = "CONFIRMED" if confirmed is True else ("PARTIAL" if confirmed is None else "REJECTED")
    repair_tag = f"  [repaired x{attempts_count-1}]" if attempts_count > 1 else ""

    print(f"\n{'='*60}")
    print(f"ITERATION {iteration} | Score: {score}/100 | {status}{repair_tag}")
    print(f"{'='*60}")
    print(f"Hypothesis : {_safe(result.get('hypothesis', '?'))}")
    print(f"Finding    : {_safe(result.get('finding', '?'))}")
    print(f"Signal     : {_safe(result.get('signal_brief', '?'))}")
    print(f"Next       : {_safe(result.get('next_hypothesis', '?'))}")


def run(iterations: int = 3, start_hint: str = "", max_seconds: int = 0):
    """Run the Ouroboros cycle for N iterations (or until max_seconds elapsed)."""
    session_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
    t_start = time.time()
    time_limit = f"  time limit: {max_seconds}s" if max_seconds else ""

    print(f"\nSignal Mind Agent — Ouroboros loop ({iterations} iterations{time_limit})")
    print(f"Session: {session_id}")
    print(f"Signals -> {SIGNALS_FILE}")

    # --- AWAKENING: load all persistent memory ---
    principles = load_principles()
    knowledge  = load_knowledge()
    pv         = _principles_version(principles)
    kl         = len(knowledge.splitlines()) if knowledge else 0

    if principles:
        print(f"[constitution] v={pv}  ({len(principles.splitlines())} lines)")
    if knowledge:
        print(f"[knowledge]    {kl} lines from past sessions")

    metrics = SessionMetrics()
    session_signals: list[dict] = []
    context = start_hint

    blacklist      = _load_blacklist()
    topic_counter: dict[str, int] = {}
    if blacklist:
        print(f"[blacklist] Loaded {len(blacklist)} cross-session convergence patterns")

    for i in range(1, iterations + 1):
        if max_seconds and (time.time() - t_start) >= max_seconds:
            print(f"\n[time limit] {max_seconds}s reached after {i-1} iterations — stopping gracefully.")
            break

        lag_days = LAG_SWEEP[(i - 1) % len(LAG_SWEEP)]
        lag_tag  = f" [lag={lag_days}d]" if lag_days else ""
        elapsed  = int(time.time() - t_start)
        print(f"\n--- Iteration {i}/{iterations}{lag_tag}  (+{elapsed}s) ---")

        # Anti-convergence: block repeated and blacklisted hypothesis fingerprints
        fp = _fingerprint(context)
        if fp:
            if fp in blacklist:
                topic   = random.choice(TOPIC_POOL)
                context = topic
                print(f"  [blacklist] Skipping known convergence trap → {_safe(topic[:80])}")
            else:
                topic_counter[fp] = topic_counter.get(fp, 0) + 1
                if topic_counter[fp] >= CONVERGENCE_SESSION_LIMIT:
                    topic   = random.choice(TOPIC_POOL)
                    context = topic
                    topic_counter.pop(fp, None)
                    print(f"  [anti-conv] {CONVERGENCE_SESSION_LIMIT}x repeat → {_safe(topic[:80])}")

        # --- LOOP 1: execute with SQL self-repair ---
        try:
            result, attempts, tel = run_hypothesis_cycle(
                hypothesis_hint=context,
                principles=principles,
                knowledge=knowledge,
                lag_days=lag_days,
            )
        except Exception as e:
            safe_err = str(e).encode("utf-8", errors="replace").decode("utf-8")
            print(f"  Fatal error: {safe_err}")
            continue

        sql_ok        = attempts[-1][1] is None
        sql_error_flag = not sql_ok

        # --- LOOP 2: journal + metrics + dataset ---
        save_signal(result)
        save_journal_entry(result, i)

        experiments.save(
            session_id=session_id,
            iteration=i,
            result=result,
            attempts=attempts,
            knowledge_lines=kl,
            principles_version=pv,
            lag_days=lag_days,
        )

        # Save per-iteration telemetry (tokens, timing, context sizes)
        tel.session_id = session_id
        tel.iteration  = i
        tel_module.save(tel)

        # Print telemetry summary line
        tok_total = tel.tok_gen_in + tel.tok_gen_out + tel.tok_eval_in + tel.tok_eval_out
        print(f"  [tel] {tel.t_total_ms//1000}s | tok={tok_total} "
              f"(gen={tel.tok_gen_in}+{tel.tok_gen_out}, eval={tel.tok_eval_in}+{tel.tok_eval_out}) "
              f"| rag={tel.t_rag_ms}ms news={tel.t_news_ms}ms sql={tel.t_sql_ms}ms")

        # If confirmed, save SQL to sql_patterns.md
        if result.get("confirmed") is True and sql_ok:
            experiments.save_sql_pattern(
                hypothesis=result.get("hypothesis", ""),
                sql=attempts[-1][0],
                signal_score=result.get("signal_score", 0) or 0,
            )
            print(f"  [patterns] SQL saved to sql_patterns.md")

        # Track generated hypothesis fingerprint for convergence detection
        hyp_fp = _fingerprint(result.get("hypothesis", ""))
        if hyp_fp:
            topic_counter[hyp_fp] = topic_counter.get(hyp_fp, 0) + 1

        metrics.record(result, sql_error=sql_error_flag)
        session_signals.append(result)

        print_result(result, i, len(attempts))

        # --- REFLECT every REFLECT_EVERY iterations (temporally separated) ---
        if i % REFLECT_EVERY == 0:
            print(f"\n[reflection] Running meta-reflection on last {REFLECT_EVERY} iterations...")
            try:
                r = reflect(session_signals[-REFLECT_EVERY:])
                print(_safe(format_reflection(r), width=2000))
            except Exception as e:
                print(f"  [reflection error] {e}")
                r = {}
            # After reflection: force next topic from pool (cycling rotation)
            topic = TOPIC_POOL[(i // REFLECT_EVERY - 1) % len(TOPIC_POOL)]
            context = topic
            print(f"  [topic] {_safe(topic[:80])}")
        else:
            # Check if generated hypothesis itself is a convergence trap
            if hyp_fp and (hyp_fp in blacklist or topic_counter.get(hyp_fp, 0) >= CONVERGENCE_SESSION_LIMIT):
                topic   = random.choice(TOPIC_POOL)
                context = topic
                reason  = "blacklist" if hyp_fp in blacklist else f"{topic_counter[hyp_fp]}x repeat"
                print(f"  [anti-conv/{reason}] Forcing next jump → {_safe(topic[:80])}")
            elif random.random() < RANDOM_JUMP_PROB:
                topic   = random.choice(TOPIC_POOL)
                context = topic
                print(f"  [random jump] {_safe(topic[:80])}")
            else:
                context = result.get("next_hypothesis", "")

        time.sleep(1)

    # --- END OF SESSION ---
    print("\n[memory] Updating knowledge base...")
    written = update_knowledge(session_signals)
    if written:
        print("  knowledge.md updated with confirmed findings")
    else:
        print("  No confirmed findings this session — knowledge.md unchanged")

    metrics.save()
    print(metrics.report())

    # Cross-session totals from experiments.db
    totals = experiments.total_stats()
    print(f"\n[dataset] Total experiments: {totals['total']} across {totals['sessions']} sessions")
    print(f"          Confirmed: {totals['confirmed']}  |  Repaired SQL: {totals['total_repaired']}  |  Failed SQL: {totals['total_failed']}")

    # Telemetry session summary
    tsummary = tel_module.session_summary(session_id)
    if tsummary:
        print(f"\n[telemetry] Session summary:")
        print(f"  Wall time      : {tsummary['total_wall_s']}s  |  avg/iter: {tsummary['avg_iter_s']}s")
        print(f"  Total tokens   : {tsummary['total_tokens']:,}  |  avg/iter: {tsummary['avg_tokens_per_iter']:.0f}")
        print(f"  Avg step times : rag={tsummary['avg_rag_ms']:.0f}ms  news={tsummary['avg_news_ms']:.0f}ms  "
              f"sql={tsummary['avg_sql_ms']:.0f}ms  llm_gen={tsummary['avg_llm_gen_ms']:.0f}ms  "
              f"llm_eval={tsummary['avg_llm_eval_ms']:.0f}ms")
        print(f"  Lag dist       : {tsummary['lag_distribution']}")
        print(f"  SQL repair rate: {tsummary['sql_repair_rate']}%")

    print(f"\nDone. {metrics.total} iterations complete.")


if __name__ == "__main__":
    import sys
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    n       = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    hint    = sys.argv[2] if len(sys.argv) > 2 else ""
    max_sec = int(sys.argv[3]) if len(sys.argv) > 3 else 0
    run(iterations=n, start_hint=hint, max_seconds=max_sec)
