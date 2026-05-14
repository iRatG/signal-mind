"""Data audit for Experiment v1 — pre-sample diagnostics across three windows.

Computes coverage, sample size, regime metrics, and language balance for
Train / Validation / Test windows before any signal-finding starts.

Output: analytics/data_audit_v1.md (markdown table) + dict for programmatic use.
"""
from __future__ import annotations

import sqlite3
import statistics
import sys
from dataclasses import dataclass
from pathlib import Path

import duckdb

ROOT = Path(__file__).parents[1]
DUCKDB_PATH = ROOT / "db" / "signal_mind.duckdb"
HF_NEWS_PATH = ROOT / "db" / "hf_news.db"
OUTPUT_MD = ROOT / "analytics" / "data_audit_v1.md"

WINDOWS = [
    ("Train",      "2022-01-01", "2023-09-30"),
    ("Validation", "2024-01-01", "2025-04-30"),
    ("Test",       "2025-09-01", "2026-04-29"),
]
BUFFERS = [
    ("buf_train_val",  "2023-10-01", "2023-12-31"),
    ("buf_val_test",   "2025-05-01", "2025-08-31"),
]

TOPICS = ["oil", "rate", "ruble", "sanctions", "inflation", "banking", "gold"]


@dataclass
class WindowMetrics:
    name: str
    start: str
    end: str
    n_trading_days: int
    n_news_total: int
    n_news_per_topic: dict
    coverage_pct: dict
    n_market_obs: int
    missing_pct: dict
    rate_min: float
    rate_median: float
    rate_max: float
    usdrub_min: float
    usdrub_median: float
    usdrub_max: float
    imoex_vol_daily_pct: float
    news_lang_ratio: dict


def period_to_date(period: str) -> str:
    """'9.2025' -> '2025-09-01' (first day of month)."""
    mo, yr = period.split(".")
    return f"{int(yr):04d}-{int(mo):02d}-01"


def audit_window(duck_con, sqlite_con, name: str, start: str, end: str) -> WindowMetrics:
    # n_trading_days from v_market_context
    n_trading_days = duck_con.execute(
        "SELECT COUNT(DISTINCT trade_date) FROM v_market_context "
        "WHERE trade_date BETWEEN ? AND ?",
        [start, end],
    ).fetchone()[0]

    # n_market_obs
    n_market_obs = duck_con.execute(
        "SELECT COUNT(*) FROM v_market_context "
        "WHERE trade_date BETWEEN ? AND ?",
        [start, end],
    ).fetchone()[0]

    # missing_pct per field
    missing_pct = {}
    for col in ["imoex_close", "usd_rub", "brent_usd", "gold_usd", "key_rate_pct"]:
        missing = duck_con.execute(
            f"SELECT COUNT(*) FROM v_market_context "
            f"WHERE trade_date BETWEEN ? AND ? AND {col} IS NULL",
            [start, end],
        ).fetchone()[0]
        missing_pct[col] = round(100.0 * missing / max(n_market_obs, 1), 2)

    # n_news_total from hf_news.db (SQLite)
    n_news_total = sqlite_con.execute(
        "SELECT COUNT(*) FROM articles WHERE DATE(date) BETWEEN ? AND ?",
        (start, end),
    ).fetchone()[0]

    # news_lang_ratio: 'data'+'en_archive:*' = EN, 'ru_archive:*' = RU
    rows = sqlite_con.execute(
        """
        SELECT
          CASE
            WHEN source = 'data' OR source LIKE 'en_archive:%' THEN 'en'
            WHEN source LIKE 'ru_archive:%' THEN 'ru'
            ELSE 'other'
          END AS lang,
          COUNT(*)
        FROM articles WHERE DATE(date) BETWEEN ? AND ?
        GROUP BY lang
        """,
        (start, end),
    ).fetchall()
    news_lang_ratio = {lang: cnt for lang, cnt in rows}
    total = sum(news_lang_ratio.values()) or 1
    news_lang_ratio_pct = {k: round(100.0 * v / total, 1) for k, v in news_lang_ratio.items()}

    # n_news_per_topic + coverage_pct from news_daily (DuckDB)
    n_news_per_topic = {}
    coverage_pct = {}
    for topic in TOPICS:
        r = duck_con.execute(
            f"SELECT SUM({topic}), "
            f"       COUNT(*) FILTER (WHERE {topic} > 0), "
            f"       COUNT(*) "
            f"FROM news_daily WHERE news_date BETWEEN ? AND ?",
            [start, end],
        ).fetchone()
        n_news_per_topic[topic] = int(r[0] or 0)
        coverage_pct[topic] = round(100.0 * (r[1] or 0) / max(r[2] or 1, 1), 1)

    # rate range — match key_rate periods that fall in window
    rates = duck_con.execute(
        """
        SELECT rate_pct FROM key_rate
        WHERE STRPTIME(period, '%-m.%Y') >= STRPTIME(?, '%Y-%m-%d')
          AND STRPTIME(period, '%-m.%Y') <= STRPTIME(?, '%Y-%m-%d')
          AND rate_pct IS NOT NULL
        """,
        [start, end],
    ).fetchall()
    rate_vals = [r[0] for r in rates]
    if not rate_vals:
        rate_min = rate_median = rate_max = float("nan")
    else:
        rate_min = min(rate_vals)
        rate_max = max(rate_vals)
        rate_median = statistics.median(rate_vals)

    # usdrub range
    usd = duck_con.execute(
        "SELECT rate FROM forex_cbr "
        "WHERE currency='USD' AND trade_date BETWEEN ? AND ? AND rate IS NOT NULL",
        [start, end],
    ).fetchall()
    usd_vals = [r[0] for r in usd]
    if usd_vals:
        usdrub_min = min(usd_vals)
        usdrub_max = max(usd_vals)
        usdrub_median = statistics.median(usd_vals)
    else:
        usdrub_min = usdrub_median = usdrub_max = float("nan")

    # imoex daily volatility: stddev of daily pct returns
    rets = duck_con.execute(
        """
        WITH ord AS (
          SELECT trade_date, imoex_close,
                 LAG(imoex_close) OVER (ORDER BY trade_date) AS prev
          FROM v_market_context
          WHERE trade_date BETWEEN ? AND ? AND imoex_close IS NOT NULL
        )
        SELECT (imoex_close / prev - 1.0) AS ret
        FROM ord WHERE prev IS NOT NULL AND prev > 0
        """,
        [start, end],
    ).fetchall()
    ret_vals = [r[0] for r in rets]
    imoex_vol = round(100.0 * statistics.stdev(ret_vals), 3) if len(ret_vals) > 1 else float("nan")

    return WindowMetrics(
        name=name, start=start, end=end,
        n_trading_days=n_trading_days,
        n_news_total=n_news_total,
        n_news_per_topic=n_news_per_topic,
        coverage_pct=coverage_pct,
        n_market_obs=n_market_obs,
        missing_pct=missing_pct,
        rate_min=rate_min, rate_median=rate_median, rate_max=rate_max,
        usdrub_min=usdrub_min, usdrub_median=usdrub_median, usdrub_max=usdrub_max,
        imoex_vol_daily_pct=imoex_vol,
        news_lang_ratio=news_lang_ratio_pct,
    )


def render_md(metrics: list[WindowMetrics]) -> str:
    lines = []
    lines.append("# Data Audit — Experiment v1")
    lines.append("")
    lines.append("Pre-sample diagnostics across Train / Validation / Test windows.")
    lines.append("Computed before any signal-finding starts. Source: signal_mind.duckdb + hf_news.db.")
    lines.append("")

    # Top: window definitions
    lines.append("## Windows")
    lines.append("")
    lines.append("| Window | Start | End | Trading days |")
    lines.append("|---|---|---|---|")
    for m in metrics:
        lines.append(f"| {m.name} | {m.start} | {m.end} | {m.n_trading_days} |")
    lines.append("")

    lines.append("Buffer zones (excluded, prevent lag-leak between windows):")
    lines.append("")
    for name, s, e in BUFFERS:
        lines.append(f"- `{name}`: {s} → {e}")
    lines.append("")

    # Market data integrity
    lines.append("## Market data integrity")
    lines.append("")
    lines.append("| Field | " + " | ".join(m.name for m in metrics) + " |")
    lines.append("|---|" + "---|" * len(metrics))
    lines.append("| n_market_obs | " + " | ".join(str(m.n_market_obs) for m in metrics) + " |")
    for col in ["imoex_close", "usd_rub", "brent_usd", "gold_usd", "key_rate_pct"]:
        row = [f"{m.missing_pct[col]}%" for m in metrics]
        lines.append(f"| missing {col} | " + " | ".join(row) + " |")
    lines.append("")

    # News coverage
    lines.append("## News coverage")
    lines.append("")
    lines.append("| Metric | " + " | ".join(m.name for m in metrics) + " |")
    lines.append("|---|" + "---|" * len(metrics))
    lines.append("| n_news_total | " + " | ".join(f"{m.n_news_total:,}" for m in metrics) + " |")
    for lang in ["en", "ru", "other"]:
        row = [f"{m.news_lang_ratio.get(lang, 0)}%" for m in metrics]
        lines.append(f"| lang_share_{lang} | " + " | ".join(row) + " |")
    lines.append("")

    lines.append("### Per-topic mentions and day-coverage")
    lines.append("")
    lines.append("| Topic | " + " | ".join(f"{m.name} mentions / coverage" for m in metrics) + " |")
    lines.append("|---|" + "---|" * len(metrics))
    for topic in TOPICS:
        row = [f"{m.n_news_per_topic[topic]:,} / {m.coverage_pct[topic]}%" for m in metrics]
        lines.append(f"| {topic} | " + " | ".join(row) + " |")
    lines.append("")
    lines.append("**Rule:** if coverage_pct < 30% in any window, the topic is marked unusable in that window.")
    lines.append("")

    # Regime metrics
    lines.append("## Regime metrics")
    lines.append("")
    lines.append("| Metric | " + " | ".join(m.name for m in metrics) + " |")
    lines.append("|---|" + "---|" * len(metrics))
    lines.append(
        "| key_rate min/median/max (%) | "
        + " | ".join(f"{m.rate_min:.2f} / {m.rate_median:.2f} / {m.rate_max:.2f}" for m in metrics)
        + " |"
    )
    lines.append(
        "| USD/RUB min/median/max | "
        + " | ".join(f"{m.usdrub_min:.2f} / {m.usdrub_median:.2f} / {m.usdrub_max:.2f}" for m in metrics)
        + " |"
    )
    lines.append(
        "| IMOEX daily vol (%) | "
        + " | ".join(f"{m.imoex_vol_daily_pct:.3f}" for m in metrics)
        + " |"
    )
    lines.append("")

    # Disqualified topics summary
    lines.append("## Topics disqualified by coverage rule")
    lines.append("")
    any_disqual = False
    for m in metrics:
        bad = [t for t in TOPICS if m.coverage_pct[t] < 30]
        if bad:
            any_disqual = True
            lines.append(f"- **{m.name}**: {', '.join(bad)}")
    if not any_disqual:
        lines.append("(none — all topics ≥ 30% coverage in all windows)")
    lines.append("")

    return "\n".join(lines)


def main():
    duck_con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    sqlite_con = sqlite3.connect(str(HF_NEWS_PATH))
    try:
        metrics = [audit_window(duck_con, sqlite_con, name, s, e) for name, s, e in WINDOWS]
    finally:
        duck_con.close()
        sqlite_con.close()

    md = render_md(metrics)
    OUTPUT_MD.write_text(md, encoding="utf-8")
    print(md)
    print(f"\n[ok] written: {OUTPUT_MD}")


if __name__ == "__main__":
    main()
