"""Fine-grained lag scan: every lag 1..90 for all instrument x topic pairs.

Does NOT require full AND confirmation — saves raw m5+m6 statistics for
each hypothesis so tomorrow we can see the EXACT lag at which signals
peak. Also runs a less strict confirmation: p_m5 < 0.05 OR m6 confirmed.

Output:
    analytics/phase_b/finegrained_<ts>_results.csv
    analytics/phase_b/finegrained_<ts>_report.md

Run: python -m src.pipeline_v2.finegrained_scanner
"""
from __future__ import annotations

import importlib
import io
import sys
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                  line_buffering=True)

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "db" / "signal_mind.duckdb"
OUT_DIR = ROOT / "analytics" / "phase_b"
OUT_DIR.mkdir(parents=True, exist_ok=True)

TOPICS = ["oil", "rate", "ruble", "sanctions", "inflation", "banking", "gold"]
LAGS_FINE = list(range(1, 91))      # 1..90 daily resolution

# Only instruments with enough train data (>=350 rows after join)
INSTRUMENTS = [
    "USD_RUB", "EUR_RUB", "BRENT", "SP500",
    "MSCI_WORLD", "DXY", "MSCI_INDIA",
    "CHINA_H_SHARES", "FTSE_CHINA_50",
]

M5_MODULE = "analytics.testbed.methods.m5_var"
M6_MODULE = "analytics.testbed.methods.m6_lgbm"

# Research thresholds (more permissive than production):
# m5: IRF p < 0.05 (instead of 0.01)
# m6: IC >= 0.03 with bootstrap (same)
M5_PVALUE_RESEARCH = 0.05


def load_split(split: str, instrument: str) -> pd.DataFrame | None:
    try:
        import duckdb
    except ImportError:
        raise RuntimeError("duckdb not installed")
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        news_df = con.execute(f"SELECT * FROM v_{split}_news").fetchdf()
        news_df = news_df.rename(columns={"news_date": "date"})
        news_df["date"] = pd.to_datetime(news_df["date"])
        mkt_df = con.execute(
            f"SELECT trade_date AS date, close FROM v_{split}_market_data "
            f"WHERE instrument = '{instrument}' ORDER BY trade_date"
        ).fetchdf()
        if len(mkt_df) < 30:
            return None
        mkt_df["date"] = pd.to_datetime(mkt_df["date"])
        mkt_df = mkt_df.sort_values("date").reset_index(drop=True)
        mkt_df["market_return"] = np.log(mkt_df["close"]).diff()
        kr_df = con.execute(
            "SELECT period_date AS date, rate_pct AS key_rate_pct "
            "FROM v_key_rate_daily ORDER BY period_date"
        ).fetchdf()
        kr_df["date"] = pd.to_datetime(kr_df["date"])
        mkt_df = pd.merge_asof(mkt_df.sort_values("date"),
                               kr_df.sort_values("date"),
                               on="date", direction="backward")
        df = pd.merge(mkt_df, news_df, on="date", how="inner")
        df = df.sort_values("date").reset_index(drop=True)
    finally:
        con.close()
    if len(df) < 50:
        return None
    return df


def run_ensemble_raw(df, topic, lag, m5, m6) -> dict:
    """Return raw statistics without any gate decision."""
    from analytics.testbed.methods.base import Hypothesis
    hyp = Hypothesis(news_field=topic, target_field="market_return", lag_days=lag)
    try:
        v5 = m5.evaluate(df, hyp)
    except Exception as e:
        return {"m5_confirmed": False, "m5_pvalue": 1.0, "m5_score": 0.0,
                "m6_confirmed": False, "m6_score": 0.0, "m6_pvalue": 1.0,
                "m5_n": 0, "m6_n": 0, "error": str(e)}
    try:
        v6 = m6.evaluate(df, hyp)
    except Exception as e:
        return {"m5_confirmed": v5.confirmed,
                "m5_pvalue": float(v5.p_value or 1.0),
                "m5_score": float(v5.score),
                "m6_confirmed": False, "m6_score": 0.0, "m6_pvalue": 1.0,
                "m5_n": v5.n, "m6_n": 0, "error": str(e)}
    return {
        "m5_confirmed": v5.confirmed,
        "m5_pvalue": round(float(v5.p_value or 1.0), 6),
        "m5_score": round(float(v5.score), 6),
        "m5_n": v5.n,
        "m6_confirmed": v6.confirmed,
        "m6_score": round(float(v6.score), 6),
        "m6_pvalue": round(float(v6.p_value or 1.0), 6),
        "m6_n": v6.n,
        "error": "",
        # Research-threshold confirmations (less strict than production)
        "research_m5": float(v5.p_value or 1.0) < M5_PVALUE_RESEARCH,
        "research_both": (float(v5.p_value or 1.0) < M5_PVALUE_RESEARCH
                          and v6.confirmed),
        "production_both": v5.confirmed and v6.confirmed,
    }


def main() -> None:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    start = time.time()
    total = len(INSTRUMENTS) * len(TOPICS) * len(LAGS_FINE)
    print(f"[{ts}] Fine-grained lag scan: {total} hypotheses "
          f"({len(INSTRUMENTS)} instruments x {len(TOPICS)} topics x "
          f"{len(LAGS_FINE)} lags)")

    m5 = importlib.import_module(M5_MODULE).build()
    m6 = importlib.import_module(M6_MODULE).build()

    # Pre-load train data (only one split for speed)
    dfs: dict[str, pd.DataFrame | None] = {}
    for inst in INSTRUMENTS:
        dfs[inst] = load_split("train", inst)
        n = len(dfs[inst]) if dfs[inst] is not None else 0
        print(f"  {inst}: {n} rows")

    rows: list[dict] = []
    done = 0
    for inst in INSTRUMENTS:
        df = dfs[inst]
        if df is None:
            continue
        for topic in TOPICS:
            if topic not in df.columns:
                continue
            for lag in LAGS_FINE:
                result = run_ensemble_raw(df, topic, lag, m5, m6)
                rows.append({
                    "instrument": inst, "topic": topic, "lag": lag,
                    **result
                })
                done += 1
                if done % 500 == 0:
                    elapsed = time.time() - start
                    eta = elapsed / done * (total - done)
                    print(f"  progress {done}/{total} "
                          f"({100*done/total:.0f}%) "
                          f"eta={eta/60:.1f}min")

    result_df = pd.DataFrame(rows)
    csv_path = OUT_DIR / f"finegrained_{ts}_results.csv"
    result_df.to_csv(csv_path, index=False)

    # Aggregate: best lag per (instrument, topic) by m5 p-value
    best = (result_df.sort_values("m5_pvalue")
            .groupby(["instrument", "topic"])
            .first()
            .reset_index()
            [["instrument", "topic", "lag", "m5_pvalue", "m5_score",
              "m6_score", "m6_pvalue", "research_m5", "research_both",
              "production_both"]])
    best = best.sort_values("m5_pvalue").reset_index(drop=True)

    prod_confirmed = result_df[result_df["production_both"]].copy()
    research_confirmed = result_df[result_df["research_both"]].copy()

    elapsed = time.time() - start
    md_path = OUT_DIR / f"finegrained_{ts}_report.md"
    lines: list[str] = [
        "# Fine-grained Lag Scan Report",
        "",
        f"Run: {datetime.now(timezone.utc).isoformat()}",
        f"Elapsed: {elapsed:.0f}s ({elapsed/60:.1f} min)",
        f"Hypotheses: {total} ({len(INSTRUMENTS)} inst x "
        f"{len(TOPICS)} topics x {len(LAGS_FINE)} lags 1..90)",
        "",
        "## Summary",
        "",
        f"| Mode | Confirmed | Note |",
        f"|---|---|---|",
        f"| Production (AND, p<0.01) | {len(prod_confirmed)} | strict |",
        f"| Research (AND, p<0.05) | {len(research_confirmed)} | exploratory |",
        "",
    ]

    if len(research_confirmed) > 0:
        lines += [
            "## Research Signals (m5 p<0.05 AND m6 IC>0.03)",
            "",
            "| Instrument | Topic | Lag | m5_p | m5_score | m6_IC | m6_p |",
            "|---|---|---|---|---|---|---|",
        ]
        for _, r in research_confirmed.iterrows():
            lines.append(
                f"| {r['instrument']} | {r['topic']} | {r['lag']} "
                f"| {r['m5_pvalue']:.4f} | {r['m5_score']:.5f} "
                f"| {r['m6_score']:.4f} | {r['m6_pvalue']:.4f} |"
            )
        lines.append("")
    else:
        lines += ["## No research signals found", ""]

    lines += [
        "## Best Lag Per (Instrument, Topic) — Top 30 by m5 p-value",
        "",
        "| Instrument | Topic | Best_Lag | m5_p | m6_IC |",
        "|---|---|---|---|---|",
    ]
    for _, r in best.head(30).iterrows():
        lines.append(
            f"| {r['instrument']} | {r['topic']} | {r['lag']} "
            f"| {r['m5_pvalue']:.4f} | {r['m6_score']:.4f} |"
        )
    lines += [
        "",
        f"Full data: `{csv_path.name}`",
    ]

    md_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nCSV: {csv_path}")
    print(f"Report: {md_path}")
    print(f"Production confirmed: {len(prod_confirmed)}")
    print(f"Research confirmed: {len(research_confirmed)}")
    print(f"Done in {elapsed/60:.1f} min")


if __name__ == "__main__":
    main()
