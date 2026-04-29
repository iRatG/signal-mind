"""Hypothesis generator + SQL verifier for Signal Mind agent."""
import re
import json

from src.agent.llm import chat
from src.agent.schema import get_schema
from src.db.init_db import get_connection


SYSTEM_PROMPT = """You are a quantitative analyst for a Russian financial signal detection system.
Your job is to generate data-driven hypotheses about Russian financial markets and macroeconomics,
then verify them with SQL queries against a DuckDB database.

Always respond in JSON. Be concise and data-focused.
Hypotheses should be specific, falsifiable, and actionable for investors or analysts."""


def generate_hypothesis(context: str = "") -> dict:
    """Ask LLM to generate a new market hypothesis."""
    schema = get_schema()
    prompt = f"""Given this database schema:

{schema}

Generate ONE specific, testable financial hypothesis about Russian markets or macroeconomics.
Focus on: correlations, regime changes, sector divergences, rate impacts, or wage/inflation dynamics.

Respond with JSON only:
{{
  "hypothesis": "short description in Russian",
  "rationale": "why this might be true (1-2 sentences)",
  "sql": "DuckDB SQL query to verify this hypothesis (must return numeric results)",
  "expected_signal": "what result would confirm the hypothesis"
}}

{f'Context from previous findings: {context}' if context else ''}"""

    response = chat([
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": prompt},
    ])

    # Extract JSON from response
    match = re.search(r'\{.*\}', response, re.DOTALL)
    if match:
        return json.loads(match.group())
    raise ValueError(f"Could not parse JSON from response: {response[:200]}")


def execute_sql(sql: str) -> tuple[list, list]:
    """Execute SQL and return (columns, rows)."""
    con = get_connection()
    try:
        result = con.execute(sql)
        cols = [d[0] for d in result.description]
        rows = result.fetchall()
        return cols, rows
    finally:
        con.close()


def evaluate_result(hypothesis: dict, cols: list, rows: list) -> dict:
    """Ask LLM to interpret the SQL results."""
    if not rows:
        data_str = "Query returned no rows."
    else:
        header = " | ".join(cols)
        body = "\n".join(" | ".join(str(v) for v in row) for row in rows[:20])
        data_str = f"{header}\n{body}"
        if len(rows) > 20:
            data_str += f"\n... ({len(rows)} rows total)"

    prompt = f"""Hypothesis: {hypothesis['hypothesis']}
Expected signal: {hypothesis['expected_signal']}

SQL result:
{data_str}

Evaluate this hypothesis based on the data. Respond with JSON only:
{{
  "confirmed": true/false/null,
  "signal_score": 0-100,
  "finding": "what the data actually shows (2-3 sentences in Russian)",
  "signal_brief": "actionable insight for investor/analyst (1-2 sentences in Russian)",
  "next_hypothesis": "suggested follow-up hypothesis based on this finding"
}}"""

    response = chat([
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": prompt},
    ])

    match = re.search(r'\{.*\}', response, re.DOTALL)
    if match:
        return json.loads(match.group())
    raise ValueError(f"Could not parse JSON: {response[:200]}")


def run_hypothesis_cycle(hypothesis_hint: str = "") -> dict:
    """Full cycle: generate → SQL → execute → evaluate."""
    print("Generating hypothesis...")
    hyp = generate_hypothesis(hypothesis_hint)
    print(f"  Hypothesis: {hyp.get('hypothesis', '?')}")
    print(f"  SQL: {hyp.get('sql', '')[:100]}...")

    print("Executing SQL...")
    try:
        cols, rows = execute_sql(hyp["sql"])
        print(f"  Result: {len(rows)} rows, cols={cols}")
    except Exception as e:
        print(f"  SQL error: {e}")
        cols, rows = [], []

    print("Evaluating...")
    evaluation = evaluate_result(hyp, cols, rows)

    result = {**hyp, **evaluation, "sql_cols": cols, "sql_rows": rows[:10]}
    return result
