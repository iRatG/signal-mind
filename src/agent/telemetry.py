"""Per-iteration telemetry for scientific analysis of agent runs.

Captures timing, token usage, and context sizes for all three data layers.
Saved to db/telemetry.jsonl — one JSON record per iteration.

Use after the run:
    import json
    records = [json.loads(l) for l in open('db/telemetry.jsonl')]
"""
import json
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path

TELEMETRY_FILE = Path(__file__).parents[2] / "db" / "telemetry.jsonl"


@dataclass
class IterationTelemetry:
    # Identity
    session_id:  str = ""
    iteration:   int = 0
    lag_days:    int = 0
    ts:          str = ""

    # Timing (ms)
    t_rag_ms:        int = 0   # Layer 2: ChromaDB retrieval
    t_news_ms:       int = 0   # Layer 3: hf_news.db LIKE query
    t_llm_gen_ms:    int = 0   # LLM: generate hypothesis
    t_sql_ms:        int = 0   # Layer 1: DuckDB SQL execution (all attempts)
    t_llm_eval_ms:   int = 0   # LLM: evaluate result
    t_total_ms:      int = 0   # wall-clock total

    # Tokens (DeepSeek API)
    tok_gen_in:    int = 0
    tok_gen_out:   int = 0
    tok_eval_in:   int = 0
    tok_eval_out:  int = 0

    # Context sizes (chars) — how much each layer contributed
    ctx_rag_fragments:  int = 0
    ctx_rag_chars:      int = 0
    ctx_news_articles:  int = 0
    ctx_news_chars:     int = 0
    ctx_schema_chars:   int = 0
    ctx_knowledge_chars:int = 0
    ctx_principles_chars:int = 0

    # SQL
    sql_attempts:  int = 1
    sql_success:   bool = True
    sql_rows:      int = 0

    # Result
    signal_score:  int = 0
    confirmed:     object = None   # True / False / None


def _ms(start: float) -> int:
    return int((time.time() - start) * 1000)


def save(tel: IterationTelemetry):
    """Append one telemetry record to telemetry.jsonl."""
    tel.ts = datetime.now().isoformat()
    tel.tok_gen_total  = tel.tok_gen_in  + tel.tok_gen_out   # type: ignore[attr-defined]
    tel.tok_eval_total = tel.tok_eval_in + tel.tok_eval_out  # type: ignore[attr-defined]
    tel.tok_session_total = tel.tok_gen_in + tel.tok_gen_out + tel.tok_eval_in + tel.tok_eval_out  # type: ignore[attr-defined]

    record = asdict(tel)
    # Add derived fields that dataclass doesn't know about
    record["tok_gen_total"]      = tel.tok_gen_in  + tel.tok_gen_out
    record["tok_eval_total"]     = tel.tok_eval_in + tel.tok_eval_out
    record["tok_session_total"]  = record["tok_gen_total"] + record["tok_eval_total"]

    TELEMETRY_FILE.parent.mkdir(exist_ok=True)
    with open(TELEMETRY_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


def session_summary(session_id: str) -> dict:
    """Read telemetry.jsonl and summarise one session."""
    if not TELEMETRY_FILE.exists():
        return {}
    records = [
        json.loads(l) for l in TELEMETRY_FILE.read_text(encoding="utf-8").splitlines()
        if l.strip()
    ]
    sess = [r for r in records if r.get("session_id") == session_id]
    if not sess:
        return {}

    n = len(sess)
    return {
        "iterations":         n,
        "total_wall_s":       round(sum(r["t_total_ms"] for r in sess) / 1000, 1),
        "avg_iter_s":         round(sum(r["t_total_ms"] for r in sess) / n / 1000, 1),
        "total_tokens":       sum(r.get("tok_session_total", 0) for r in sess),
        "avg_tokens_per_iter":round(sum(r.get("tok_session_total", 0) for r in sess) / n, 0),
        "avg_rag_ms":         round(sum(r["t_rag_ms"]  for r in sess) / n, 0),
        "avg_news_ms":        round(sum(r["t_news_ms"] for r in sess) / n, 0),
        "avg_sql_ms":         round(sum(r["t_sql_ms"]  for r in sess) / n, 0),
        "avg_llm_gen_ms":     round(sum(r["t_llm_gen_ms"]  for r in sess) / n, 0),
        "avg_llm_eval_ms":    round(sum(r["t_llm_eval_ms"] for r in sess) / n, 0),
        "lag_distribution":   {
            str(lag): sum(1 for r in sess if r["lag_days"] == lag)
            for lag in sorted({r["lag_days"] for r in sess})
        },
        "sql_repair_rate":    round(sum(1 for r in sess if r["sql_attempts"] > 1) / n * 100, 1),
        "avg_ctx_rag_chars":  round(sum(r["ctx_rag_chars"]  for r in sess) / n, 0),
        "avg_ctx_news_chars": round(sum(r["ctx_news_chars"] for r in sess) / n, 0),
    }
