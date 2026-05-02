"""Hypothesis generator + SQL verifier with self-repair loop."""
import json
import re
import time
from pathlib import Path

from src.agent.llm import chat, chat_with_usage
from src.agent.schema import get_schema
from src.agent.sql_repair import execute_with_repair, load_forbidden_patterns, load_sql_patterns
from src.agent.rag import get_context as rag_get_context
from src.agent.news_retriever import get_news_context
from src.agent.telemetry import IterationTelemetry, _ms
from src.db.init_db import get_connection


_REGIME_PATH = Path(__file__).parents[2] / "db" / "current_regime.json"

# Mirrors revizor.py constants — used for pre-execution aliasing check
_FOREIGN_INSTRUMENTS = [
    "ftse_china_50", "dxy", "msci_india", "msci_world",
    "dj_south_africa", "china_h_shares", "silver", "sp500", "aluminum",
]
_CONTEXT_COLUMNS = [
    "imoex_close", "usd_rub", "eur_rub", "brent_usd", "gold_usd", "key_rate_pct",
]


def _load_regime() -> dict:
    """Return current market regime from db/current_regime.json (written by Revizor)."""
    try:
        return json.loads(_REGIME_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"key_rate": 21.0, "usd_rub": 85.0, "updated": "unknown"}


def _check_aliasing(sql: str) -> list[str]:
    """Detect v_market_context column aliased as foreign instrument name."""
    bugs = []
    for inst in _FOREIGN_INSTRUMENTS:
        for col in _CONTEXT_COLUMNS:
            if re.search(rf"\b{re.escape(col)}\s+AS\s+{re.escape(inst)}\b", sql, re.IGNORECASE):
                bugs.append(f"{col} AS {inst}")
    return bugs


SYSTEM_PROMPT = """You are a quantitative analyst for a Russian financial signal detection system.
Your job is to generate data-driven hypotheses about Russian financial markets and macroeconomics,
then verify them with SQL queries against a DuckDB database.

Always respond in JSON. Be concise and data-focused.
Hypotheses should be specific, falsifiable, and actionable for investors or analysts."""


def _raw_execute(sql: str) -> tuple[list, list]:
    con = get_connection()
    try:
        result = con.execute(sql)
        cols = [d[0] for d in result.description]
        rows = result.fetchall()
        return cols, rows
    finally:
        con.close()


def generate_hypothesis(
    context: str = "",
    principles: str = "",
    knowledge: str = "",
    lag_days: int = 0,
) -> dict:
    schema = get_schema()
    forbidden = load_forbidden_patterns()
    patterns = load_sql_patterns()

    forbidden_block  = f"\nForbidden SQL patterns (NEVER use these):\n{forbidden}\n" if forbidden else ""
    patterns_block   = f"\nWorking SQL templates (use as reference):\n{patterns}\n" if patterns else ""
    principles_block = f"\nAnalytical principles (must follow):\n{principles}\n" if principles else ""
    knowledge_block  = (
        f"\nAccumulated knowledge from past sessions (do NOT repeat these findings):\n{knowledge}\n"
        if knowledge else ""
    )
    context_block = f"\nContext from previous finding: {context}" if context else ""

    lag_block = (
        f"\nLAG TARGET: {lag_days} days. Generate a LAG HYPOTHESIS of the form:\n"
        f"  'Does news topic X at date T correlate with market metric Y at date T+{lag_days}?'\n"
        f"Use the news_daily table and the LAG HYPOTHESIS TEMPLATE from the schema.\n"
        f"Replace {{N}} with {lag_days}, choose topic and market_col based on context.\n"
    ) if lag_days else ""

    # Inject current regime so generator avoids inactive-regime hypotheses from the start
    _regime = _load_regime()
    regime_gen_block = (
        f"\n⚠️ CURRENT MARKET REGIME ({str(_regime.get('updated', ''))[:10]}):\n"
        f"  key_rate = {_regime['key_rate']}%  |  USD/RUB ≈ {_regime['usd_rub']}\n"
        "  RULE: Do NOT generate hypotheses that are only valid when key_rate < 15% "
        "or USD/RUB < 75 — those regimes are currently inactive.\n"
        "  Generate hypotheses valid in the CURRENT high-rate, high-USD/RUB environment.\n\n"
    )

    # Layer 2: RAG — regulatory docs + corporate reports
    rag_query = context if context else "Russian financial market signals rates sectors"
    _t_rag = time.time()
    rag_ctx = rag_get_context(rag_query, top_k=5)
    tel_rag_ms = _ms(_t_rag)
    rag_block = f"\n{rag_ctx}\n" if rag_ctx else ""

    # Layer 3: recent news (English, 2022–2025)
    news_query = context if context else "Russia financial market"
    _t_news = time.time()
    news_ctx = get_news_context(news_query, date_from="2022-01-01", date_to="2025-12-31", top_n=5)
    tel_news_ms = _ms(_t_news)
    news_block = f"\n{news_ctx}\n" if news_ctx else ""

    prompt = (
        f"Given this database schema:\n{schema}\n"
        f"{principles_block}"
        f"{knowledge_block}"
        f"{forbidden_block}"
        f"{patterns_block}"
        f"{regime_gen_block}"
        f"{rag_block}"
        f"{news_block}"
        f"{lag_block}"
        "Generate ONE specific, testable financial hypothesis about Russian markets or macroeconomics.\n"
        "Focus on: correlations, regime changes, sector divergences, rate impacts, wage/inflation dynamics.\n"
        "The hypothesis must be falsifiable by a single SQL query returning numeric results.\n\n"
        "Respond with JSON only:\n"
        "{\n"
        '  "hypothesis": "short description in Russian",\n'
        '  "rationale": "why this might be true (1-2 sentences)",\n'
        '  "sql": "DuckDB SQL query — must NOT use any forbidden patterns above",\n'
        '  "expected_signal": "what result would confirm the hypothesis"\n'
        "}"
        f"{context_block}"
    )

    _t_llm = time.time()
    response, usage = chat_with_usage([
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": prompt},
    ])
    tel_llm_ms = _ms(_t_llm)

    _tel_seed = {
        "t_rag_ms":            tel_rag_ms,
        "t_news_ms":           tel_news_ms,
        "t_llm_gen_ms":        tel_llm_ms,
        "tok_gen_in":          usage["prompt_tokens"],
        "tok_gen_out":         usage["completion_tokens"],
        "ctx_rag_chars":       len(rag_ctx),
        "ctx_rag_fragments":   rag_ctx.count("\n\n") if rag_ctx else 0,
        "ctx_news_chars":      len(news_ctx),
        "ctx_news_articles":   news_ctx.count("\n\n") if news_ctx else 0,
        "ctx_schema_chars":    len(get_schema()),
        "ctx_principles_chars":len(principles),
        "ctx_knowledge_chars": len(knowledge),
    }

    match = re.search(r"\{.*\}", response, re.DOTALL)
    if match:
        result = json.loads(match.group())
        result["_tel"] = _tel_seed
        return result
    raise ValueError(f"Could not parse JSON: {response[:200]}")


def evaluate_result(hypothesis: dict, cols: list, rows: list) -> dict:
    if not rows:
        data_str = "Query returned no rows."
    else:
        header = " | ".join(cols)
        body   = "\n".join(" | ".join(str(v) for v in row) for row in rows[:20])
        data_str = f"{header}\n{body}"
        if len(rows) > 20:
            data_str += f"\n... ({len(rows)} rows total, showing first 20)"

    # Layer 2: RAG context for narrative enrichment of the signal
    rag_ctx = rag_get_context(hypothesis.get("hypothesis", ""), top_k=3)
    rag_block = f"\nDocument context for signal narrative:\n{rag_ctx}\n\n" if rag_ctx else ""

    # Inject current market regime so evaluator can reject inactive-regime signals
    regime = _load_regime()
    regime_block = (
        f"\n⚠️ CURRENT MARKET REGIME ({str(regime.get('updated', ''))[:10]}):\n"
        f"  key_rate = {regime['key_rate']}% (HIGH RATE regime — above 15%)\n"
        f"  USD/RUB ≈ {regime['usd_rub']}\n"
        "  RULE: If this signal is ONLY valid when key_rate < 15% or USD/RUB < 75 "
        "— set confirmed=false. Signals must hold in the CURRENT regime to be actionable.\n\n"
    )

    prompt = (
        f"Hypothesis: {hypothesis['hypothesis']}\n"
        f"Expected signal: {hypothesis['expected_signal']}\n\n"
        f"SQL result:\n{data_str}\n\n"
        f"{rag_block}"
        f"{regime_block}"
        "Evaluate this hypothesis. Follow these rules STRICTLY:\n\n"
        "CONFIRMED (true): data supports the DIRECTION, even if magnitude differs.\n"
        "  Example: expected r<-0.5, found r=-0.3 with n=700 → confirmed=true, score=55.\n\n"
        "PARTIAL (null): data is insufficient (n<30), CORR() returned NULL, or 0 rows.\n\n"
        "REJECTED (false): data CONTRADICTS the direction. Even when rejecting,\n"
        "  if data shows a STRONG OPPOSITE pattern (r>0.5 or r<-0.5), set score=55-70.\n\n"
        "signal_score reflects DATA QUALITY, not hypothesis accuracy:\n"
        "  70-85: strong signal (r>0.5 or clear trend, n>200)\n"
        "  50-70: moderate (r=0.3-0.5, n>100)\n"
        "  30-50: weak but directional (n>30)\n"
        "  <30: noisy or inconclusive\n\n"
        "Respond with JSON only:\n"
        "{\n"
        '  "confirmed": true/false/null,\n'
        '  "signal_score": 0-100,\n'
        '  "finding": "what the data ACTUALLY shows, with sample size (2-3 sentences in Russian)",\n'
        '  "signal_brief": "actionable insight based on actual finding (1-2 sentences in Russian)",\n'
        '  "next_hypothesis": "follow-up that narrows or stress-tests what was ACTUALLY found"\n'
        "}"
    )

    _t_eval = time.time()
    response, usage = chat_with_usage([
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": prompt},
    ])
    tel_eval_ms = _ms(_t_eval)

    match = re.search(r"\{.*\}", response, re.DOTALL)
    if match:
        result = json.loads(match.group())
        result["_tel_eval"] = {
            "t_llm_eval_ms": tel_eval_ms,
            "tok_eval_in":   usage["prompt_tokens"],
            "tok_eval_out":  usage["completion_tokens"],
        }
        return result
    raise ValueError(f"Could not parse JSON: {response[:200]}")


def run_hypothesis_cycle(
    hypothesis_hint: str = "",
    principles: str = "",
    knowledge: str = "",
    lag_days: int = 0,
) -> tuple[dict, list[tuple], IterationTelemetry]:
    """
    Full cycle: generate → SQL (with repair) → evaluate.

    Returns:
        result    — merged hypothesis + evaluation dict
        attempts  — [(sql, error_or_None, error_type_or_None), ...]
        tel       — IterationTelemetry with timing + token data
    """
    t_total_start = time.time()
    tel = IterationTelemetry(lag_days=lag_days)

    print("  Generating hypothesis...")
    hyp = generate_hypothesis(hypothesis_hint, principles=principles, knowledge=knowledge, lag_days=lag_days)
    print(f"  Hypothesis: {hyp.get('hypothesis', '?')[:80]}")
    print(f"  SQL: {hyp.get('sql', '')[:90]}...")

    # Unpack telemetry from generate step
    tel_seed = hyp.pop("_tel", {})
    tel.t_rag_ms           = tel_seed.get("t_rag_ms", 0)
    tel.t_news_ms          = tel_seed.get("t_news_ms", 0)
    tel.t_llm_gen_ms       = tel_seed.get("t_llm_gen_ms", 0)
    tel.tok_gen_in         = tel_seed.get("tok_gen_in", 0)
    tel.tok_gen_out        = tel_seed.get("tok_gen_out", 0)
    tel.ctx_rag_chars      = tel_seed.get("ctx_rag_chars", 0)
    tel.ctx_rag_fragments  = tel_seed.get("ctx_rag_fragments", 0)
    tel.ctx_news_chars     = tel_seed.get("ctx_news_chars", 0)
    tel.ctx_news_articles  = tel_seed.get("ctx_news_articles", 0)
    tel.ctx_schema_chars   = tel_seed.get("ctx_schema_chars", 0)
    tel.ctx_principles_chars = tel_seed.get("ctx_principles_chars", 0)
    tel.ctx_knowledge_chars  = tel_seed.get("ctx_knowledge_chars", 0)

    # Pre-execution aliasing validator — catch v_market_context.X AS <foreign> before DuckDB sees it
    aliasing_bugs = _check_aliasing(hyp.get("sql", ""))
    if aliasing_bugs:
        print(f"  [pre-validator] Aliasing detected: {aliasing_bugs} — regenerating...")
        warn = (
            f"\n\n⚠️ Aliasing bug in previous attempt: {aliasing_bugs}. "
            "MANDATORY: use FROM market_data WHERE instrument='X' for foreign instruments."
        )
        hyp2 = generate_hypothesis(
            hypothesis_hint + warn, principles=principles, knowledge=knowledge, lag_days=lag_days
        )
        tel_seed2 = hyp2.pop("_tel", {})
        hyp = hyp2
        # overwrite telemetry with regenerated call's data
        tel.t_rag_ms           = tel_seed2.get("t_rag_ms", tel.t_rag_ms)
        tel.t_news_ms          = tel_seed2.get("t_news_ms", tel.t_news_ms)
        tel.t_llm_gen_ms       = tel_seed2.get("t_llm_gen_ms", tel.t_llm_gen_ms)
        tel.tok_gen_in        += tel_seed2.get("tok_gen_in", 0)
        tel.tok_gen_out       += tel_seed2.get("tok_gen_out", 0)

    print("  Executing SQL (with repair)...")
    _t_sql = time.time()
    cols, rows, attempts = execute_with_repair(_raw_execute, hyp["sql"], max_attempts=3)
    tel.t_sql_ms = _ms(_t_sql)

    final_attempt = attempts[-1]
    sql_ok = final_attempt[1] is None
    tel.sql_attempts = len(attempts)
    tel.sql_success  = sql_ok
    tel.sql_rows     = len(rows)

    if sql_ok:
        print(f"  Result: {len(rows)} rows  (attempts: {len(attempts)})")
    else:
        print(f"  Result: all {len(attempts)} attempts failed")

    print("  Evaluating...")
    evaluation = evaluate_result(hyp, cols, rows)

    tel_eval = evaluation.pop("_tel_eval", {})
    tel.t_llm_eval_ms = tel_eval.get("t_llm_eval_ms", 0)
    tel.tok_eval_in   = tel_eval.get("tok_eval_in", 0)
    tel.tok_eval_out  = tel_eval.get("tok_eval_out", 0)
    tel.t_total_ms    = _ms(t_total_start)
    tel.signal_score  = evaluation.get("signal_score", 0) or 0
    tel.confirmed     = evaluation.get("confirmed")

    result = {
        **hyp,
        **evaluation,
        "sql": attempts[-1][0],
        "sql_rows": rows[:10],
    }
    return result, attempts, tel
