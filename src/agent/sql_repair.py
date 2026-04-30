"""SQL self-repair engine — error taxonomy + LLM-guided fix loop.

Architecture:
  execute_sql(sql)
    → error → classify_error() → lookup hint → llm_repair()
    → retry (max 3 attempts)
    → record unknown errors to forbidden_patterns.md (they become knowledge)
"""
import re
from datetime import datetime
from pathlib import Path

from src.agent.llm import chat
from src.agent.schema import get_schema

FORBIDDEN_FILE = Path(__file__).parents[2] / "db" / "forbidden_patterns.md"
SQL_PATTERNS_FILE = Path(__file__).parents[2] / "db" / "sql_patterns.md"

# Known error signatures → error_type key
_TAXONOMY = {
    "window_in_where":  ["WHERE clause cannot contain window functions"],
    "lag_scalar":       ["Scalar Function with name lag", "Scalar Function with name lead"],
    "schema_column":    ["Referenced column", "not found in FROM clause", "Column with name"],
    "type_arithmetic":  ["No function matches the given name and argument types"],
    "type_cast":        ["Conversion Error", "Could not convert", "Cannot cast"],
    "catalog":          ["Catalog Error"],
    "binder":           ["Binder Error"],
    "syntax":           ["Parser Error", "syntax error"],
    "corr_null":        [],  # not an error — handled separately
}

# Static fix hints per error type
_HINTS = {
    "window_in_where": (
        "Window functions (lag, lead, row_number) CANNOT be in WHERE. "
        "Put them in a CTE first, then filter in the outer SELECT:\n"
        "WITH t AS (SELECT ..., lag(col) OVER (ORDER BY d) AS prev FROM ...) "
        "SELECT ... FROM t WHERE col > prev"
    ),
    "lag_scalar": (
        "lag()/lead() must ALWAYS have an OVER() clause. "
        "Never write: (a - lag(a) OVER(...)) / lag(a). "
        "Instead alias in CTE: WITH t AS (SELECT a, lag(a) OVER(...) AS prev FROM ...) "
        "SELECT (a - prev) / prev * 100 FROM t WHERE prev IS NOT NULL"
    ),
    "schema_column": (
        "Check the exact column names in the schema. "
        "v_market_context has: imoex_close, usd_rub, brent_usd, gold_usd, key_rate_pct. "
        "v_moex_sectors has: moexfn_finance, moexog_oil_gas, moex10_bluechip. "
        "v_wage_dynamics has: year, avg_wage_rub, real_wage_idx. "
        "Do NOT mix columns from different views."
    ),
    "type_arithmetic": (
        "Do not add INTEGER to TIMESTAMP/DATE. "
        "For date math use INTERVAL: trade_date + INTERVAL '1 year'. "
        "For year extraction: EXTRACT(year FROM trade_date)."
    ),
    "type_cast": (
        "Use TRY_CAST instead of CAST for columns that may have non-numeric values. "
        "Especially rosstat_macro.period — may contain '1991-Jan' strings."
    ),
    "binder":  "Check that all referenced columns exist in the FROM clause. Use table aliases consistently.",
    "catalog": "Check function names. DuckDB uses: CORR(), STDDEV(), AVG(), lag(), DATE_TRUNC(), EXTRACT().",
    "syntax":  "Fix SQL syntax. Ensure CTEs use WITH ... AS (...), GROUP BY matches SELECT columns.",
}


def classify_error(error_msg: str) -> str:
    for error_type, patterns in _TAXONOMY.items():
        if any(p.lower() in error_msg.lower() for p in patterns):
            return error_type
    return "unknown"


def load_forbidden_patterns() -> str:
    return FORBIDDEN_FILE.read_text(encoding="utf-8").strip() if FORBIDDEN_FILE.exists() else ""


def load_sql_patterns() -> str:
    return SQL_PATTERNS_FILE.read_text(encoding="utf-8").strip() if SQL_PATTERNS_FILE.exists() else ""


def record_error(error_type: str, sql: str, error_msg: str):
    """Append new/unknown error patterns to forbidden_patterns.md."""
    if error_type != "unknown":
        return  # known errors are already documented
    FORBIDDEN_FILE.parent.mkdir(exist_ok=True)
    existing = load_forbidden_patterns()
    snippet = error_msg[:100]
    if snippet in existing:
        return
    ts = datetime.now().strftime("%Y-%m-%d")
    entry = (
        f"\n## [{ts}] unknown error\n"
        f"**Error:** `{error_msg[:300]}`\n\n"
        f"**SQL snippet:**\n```sql\n{sql[:400]}\n```\n"
    )
    with open(FORBIDDEN_FILE, "a", encoding="utf-8") as f:
        f.write(entry)


def repair_sql(sql: str, error_msg: str, attempt: int) -> str:
    """Ask LLM to produce a fixed version of the broken SQL."""
    schema = get_schema()
    forbidden = load_forbidden_patterns()
    error_type = classify_error(error_msg)
    hint = _HINTS.get(error_type, "Rewrite the query to avoid the error.")

    forbidden_block = f"\nKnown forbidden patterns (never repeat these):\n{forbidden}\n" if forbidden else ""

    prompt = (
        f"Fix this broken DuckDB SQL query. Return ONLY the fixed SQL, no explanation, no markdown fences.\n\n"
        f"Schema:\n{schema}\n"
        f"{forbidden_block}"
        f"Broken SQL (attempt {attempt}):\n{sql}\n\n"
        f"Error: {error_msg}\n"
        f"Error type: {error_type}\n"
        f"Fix guidance: {hint}\n\n"
        f"Fixed SQL:"
    )

    fixed = chat([{"role": "user", "content": prompt}], temperature=0.1)
    fixed = re.sub(r"```sql\s*", "", fixed)
    fixed = re.sub(r"```\s*", "", fixed)
    return fixed.strip()


def execute_with_repair(
    execute_fn,          # callable: sql → (cols, rows)
    sql: str,
    max_attempts: int = 3,
) -> tuple[list, list, list[tuple]]:
    """
    Execute SQL with self-repair loop.

    Returns:
        cols       — column names (empty list on total failure)
        rows       — result rows  (empty list on total failure)
        attempts   — list of (sql, error_or_None, error_type_or_None)
                     last entry is the final attempt
    """
    attempts: list[tuple] = []
    current_sql = sql

    for attempt_num in range(1, max_attempts + 1):
        try:
            cols, rows = execute_fn(current_sql)
            attempts.append((current_sql, None, None))
            return cols, rows, attempts
        except Exception as e:
            error_msg = str(e)
            error_type = classify_error(error_msg)
            attempts.append((current_sql, error_msg, error_type))
            record_error(error_type, current_sql, error_msg)

            if attempt_num < max_attempts:
                print(f"    [repair] attempt {attempt_num} failed ({error_type}) — fixing...")
                current_sql = repair_sql(current_sql, error_msg, attempt_num)
            else:
                print(f"    [repair] all {max_attempts} attempts failed — giving up")

    return [], [], attempts
