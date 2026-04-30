"""SQLite experiments database — structured storage for all iterations.

Every iteration is a row: hypothesis → SQL attempts → result.
This is the fine-tuning dataset that accumulates automatically.

Export methods:
  export_text2sql()   → Dataset A: (context → SQL) pairs
  export_repair()     → Dataset B: (broken SQL + error → fixed SQL) pairs
"""
import sqlite3
import json
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parents[2] / "db" / "experiments.db"

_CREATE = """
CREATE TABLE IF NOT EXISTS experiments (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id            TEXT    NOT NULL,
    iteration             INTEGER NOT NULL,
    ts                    TEXT    NOT NULL,

    -- hypothesis
    hypothesis            TEXT,
    rationale             TEXT,
    expected_signal       TEXT,

    -- SQL attempt 1
    sql_v1                TEXT,
    sql_v1_error          TEXT,
    sql_v1_error_type     TEXT,

    -- SQL attempt 2 (after first repair)
    sql_v2                TEXT,
    sql_v2_error          TEXT,
    sql_v2_error_type     TEXT,

    -- SQL attempt 3 (after second repair)
    sql_v3                TEXT,
    sql_v3_error          TEXT,
    sql_v3_error_type     TEXT,

    -- final state
    sql_final             TEXT,
    sql_attempts          INTEGER DEFAULT 1,
    sql_success           INTEGER DEFAULT 1,  -- 1=worked, 0=all failed

    -- result
    rows_count            INTEGER,
    confirmed             INTEGER,  -- 1=true, 0=false, NULL=partial
    signal_score          INTEGER,
    finding               TEXT,
    signal_brief          TEXT,
    next_hypothesis       TEXT,

    -- meta (for dataset versioning)
    principles_version    TEXT,
    knowledge_lines       INTEGER DEFAULT 0,
    lag_days              INTEGER DEFAULT 0   -- 0 = pure DuckDB hypothesis; >0 = news lag hypothesis
)
"""

_CREATE_IDX = """
CREATE INDEX IF NOT EXISTS idx_session ON experiments(session_id);
CREATE INDEX IF NOT EXISTS idx_confirmed ON experiments(confirmed, signal_score);
CREATE INDEX IF NOT EXISTS idx_repair ON experiments(sql_v1_error_type);
"""


def _conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(exist_ok=True)
    con = sqlite3.connect(str(DB_PATH))
    con.executescript(_CREATE)
    for stmt in _CREATE_IDX.strip().split(";"):
        if stmt.strip():
            con.execute(stmt)
    con.commit()
    return con


def save(
    session_id: str,
    iteration: int,
    result: dict,
    attempts: list[tuple],   # [(sql, error_or_None, error_type_or_None), ...]
    knowledge_lines: int = 0,
    principles_version: str = "",
    lag_days: int = 0,
):
    """Persist one full experiment record."""
    def _get(lst, i, field):
        return lst[i][field] if i < len(lst) and len(lst[i]) > field else None

    sqls   = [a[0] for a in attempts]
    errors = [a[1] for a in attempts]
    etypes = [a[2] for a in attempts]

    confirmed_int = None
    if result.get("confirmed") is True:
        confirmed_int = 1
    elif result.get("confirmed") is False:
        confirmed_int = 0

    rows_count = len(result.get("sql_rows") or [])

    con = _conn()
    con.execute("""
        INSERT INTO experiments (
            session_id, iteration, ts,
            hypothesis, rationale, expected_signal,
            sql_v1, sql_v1_error, sql_v1_error_type,
            sql_v2, sql_v2_error, sql_v2_error_type,
            sql_v3, sql_v3_error, sql_v3_error_type,
            sql_final, sql_attempts, sql_success,
            rows_count, confirmed, signal_score,
            finding, signal_brief, next_hypothesis,
            principles_version, knowledge_lines, lag_days
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        session_id, iteration, datetime.now().isoformat(),
        result.get("hypothesis"), result.get("rationale"), result.get("expected_signal"),
        sqls[0]  if len(sqls) > 0 else None,
        errors[0] if len(errors) > 0 else None,
        etypes[0] if len(etypes) > 0 else None,
        sqls[1]  if len(sqls) > 1 else None,
        errors[1] if len(errors) > 1 else None,
        etypes[1] if len(etypes) > 1 else None,
        sqls[2]  if len(sqls) > 2 else None,
        errors[2] if len(errors) > 2 else None,
        etypes[2] if len(etypes) > 2 else None,
        sqls[-1],
        len(attempts),
        1 if (attempts and attempts[-1][1] is None) else 0,
        rows_count,
        confirmed_int,
        result.get("signal_score"),
        result.get("finding"), result.get("signal_brief"), result.get("next_hypothesis"),
        principles_version, knowledge_lines, lag_days,
    ))
    con.commit()
    con.close()


def session_stats(session_id: str) -> dict:
    con = _conn()
    row = con.execute("""
        SELECT
            COUNT(*)                                          AS total,
            SUM(CASE WHEN confirmed=1 THEN 1 ELSE 0 END)     AS confirmed,
            SUM(CASE WHEN confirmed=0 THEN 1 ELSE 0 END)     AS rejected,
            SUM(CASE WHEN confirmed IS NULL THEN 1 ELSE 0 END) AS partial,
            ROUND(AVG(signal_score), 1)                       AS avg_score,
            SUM(CASE WHEN sql_attempts > 1 THEN 1 ELSE 0 END) AS repaired,
            SUM(CASE WHEN sql_success=0 THEN 1 ELSE 0 END)   AS failed_sql
        FROM experiments WHERE session_id = ?
    """, (session_id,)).fetchone()
    con.close()
    return {
        "total":     row[0] or 0,
        "confirmed": row[1] or 0,
        "rejected":  row[2] or 0,
        "partial":   row[3] or 0,
        "avg_score": row[4] or 0.0,
        "repaired":  row[5] or 0,   # SQL was repaired at least once
        "failed_sql": row[6] or 0,  # all repair attempts failed
    }


def total_stats() -> dict:
    con = _conn()
    row = con.execute("""
        SELECT
            COUNT(*)                                           AS total,
            SUM(CASE WHEN confirmed=1 THEN 1 ELSE 0 END)      AS confirmed,
            ROUND(AVG(CASE WHEN confirmed=1 THEN signal_score END), 1) AS avg_confirmed_score,
            SUM(CASE WHEN sql_attempts > 1 THEN 1 ELSE 0 END) AS total_repaired,
            SUM(CASE WHEN sql_success=0 THEN 1 ELSE 0 END)    AS total_failed,
            COUNT(DISTINCT session_id)                         AS sessions
        FROM experiments
    """).fetchone()
    con.close()
    return {
        "total": row[0] or 0, "confirmed": row[1] or 0,
        "avg_confirmed_score": row[2] or 0.0,
        "total_repaired": row[3] or 0, "total_failed": row[4] or 0,
        "sessions": row[5] or 0,
    }


def export_text2sql() -> list[dict]:
    """Dataset A: hypothesis + context → working SQL. For text2sql fine-tuning."""
    con = _conn()
    rows = con.execute("""
        SELECT hypothesis, rationale, expected_signal,
               sql_final, signal_score, confirmed, knowledge_lines
        FROM experiments
        WHERE sql_success = 1 AND sql_final IS NOT NULL
        ORDER BY signal_score DESC NULLS LAST
    """).fetchall()
    con.close()
    return [
        {"hypothesis": r[0], "rationale": r[1], "expected_signal": r[2],
         "sql": r[3], "signal_score": r[4], "confirmed": r[5],
         "knowledge_context_lines": r[6]}
        for r in rows
    ]


def export_repair() -> list[dict]:
    """Dataset B: broken SQL + error → fixed SQL. For sql_repair fine-tuning."""
    con = _conn()
    rows = con.execute("""
        SELECT sql_v1, sql_v1_error, sql_v1_error_type,
               sql_v2, sql_success
        FROM experiments
        WHERE sql_v1_error IS NOT NULL AND sql_v2 IS NOT NULL
        ORDER BY ts DESC
    """).fetchall()
    con.close()
    return [
        {"broken_sql": r[0], "error_msg": r[1], "error_type": r[2],
         "fixed_sql": r[3], "fix_worked": bool(r[4])}
        for r in rows
    ]


def save_sql_pattern(hypothesis: str, sql: str, signal_score: int):
    """Append a confirmed working SQL to sql_patterns.md."""
    patterns_file = Path(__file__).parents[2] / "db" / "sql_patterns.md"
    patterns_file.parent.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d")
    entry = (
        f"\n## [{ts}] score={signal_score} — {hypothesis[:80]}\n\n"
        f"```sql\n{sql}\n```\n"
    )
    with open(patterns_file, "a", encoding="utf-8") as f:
        f.write(entry)
