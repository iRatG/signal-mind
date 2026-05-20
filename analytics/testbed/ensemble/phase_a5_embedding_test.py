"""Phase A.5 (Embedding variant) — Shuffle test on embedding-based features.

Mirrors phase_a5_shuffle_test.py but uses oil_emb/rate_emb/... columns
instead of keyword counts. Validates that M5+M6/AND ensemble does NOT
overfit on embedding features (expected: 0% FPR on shuffled data).

Run:
    python -m analytics.testbed.ensemble.phase_a5_embedding_test
"""
from __future__ import annotations

import io
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import importlib

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                  line_buffering=True)

HERE = Path(__file__).resolve().parent
ROOT_TESTBED = HERE.parent
ROOT_PROJECT = ROOT_TESTBED.parents[1]
RESULTS = ROOT_TESTBED / "results"
DB_PATH = ROOT_PROJECT / "db" / "signal_mind.duckdb"

SHUFFLE_SEED = 42
INSTRUMENT = "BRENT"
EMB_TOPICS = [f"{t}_emb" for t in
              ["oil", "rate", "ruble", "sanctions", "inflation", "banking", "gold"]]
LAGS = [1, 7, 14, 30, 60, 90]


def load_real_data() -> pd.DataFrame:
    import duckdb
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        news_df = con.execute("SELECT * FROM v_train_news").fetchdf()
        news_df = news_df.rename(columns={"news_date": "date"})
        news_df["date"] = pd.to_datetime(news_df["date"])

        market_df = con.execute(
            f"SELECT trade_date AS date, close FROM v_train_market_data "
            f"WHERE instrument = '{INSTRUMENT}' ORDER BY trade_date"
        ).fetchdf()
        market_df["date"] = pd.to_datetime(market_df["date"])
        market_df = market_df.sort_values("date").reset_index(drop=True)
        market_df["market_return"] = np.log(market_df["close"]).diff()

        kr_df = con.execute(
            "SELECT period_date AS date, rate_pct AS key_rate_pct "
            "FROM v_key_rate_daily ORDER BY period_date"
        ).fetchdf()
        kr_df["date"] = pd.to_datetime(kr_df["date"])
        market_df = pd.merge_asof(
            market_df.sort_values("date"),
            kr_df.sort_values("date"),
            on="date", direction="backward",
        )
        df = pd.merge(market_df, news_df, on="date", how="inner")
        df = df.sort_values("date").reset_index(drop=True)
    finally:
        con.close()
    return df


def shuffle_emb(df: pd.DataFrame, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    df_shuf = df.copy()
    for col in EMB_TOPICS:
        if col in df_shuf.columns:
            df_shuf[col] = rng.permutation(df_shuf[col].to_numpy())
    return df_shuf


def run_ensemble(df: pd.DataFrame, topic: str, lag: int) -> bool:
    from analytics.testbed.methods.base import Hypothesis
    hyp = Hypothesis(news_field=topic, target_field="market_return", lag_days=lag)
    results = []
    for mod_name in ("analytics.testbed.methods.m5_var",
                     "analytics.testbed.methods.m6_lgbm"):
        mod = importlib.import_module(mod_name)
        verdict = mod.build().evaluate(df, hyp)
        results.append(verdict.confirmed)
    return all(results)


def main() -> float:
    print(f"[A.5-EMB] loading real train data from {DB_PATH} ...")
    df = load_real_data()
    print(f"[A.5-EMB] shape: {df.shape}")

    available = [t for t in EMB_TOPICS if t in df.columns]
    missing   = [t for t in EMB_TOPICS if t not in df.columns]
    print(f"[A.5-EMB] embedding topics found: {available}")
    if missing:
        print(f"[A.5-EMB] WARNING — missing: {missing}")

    df_shuf = shuffle_emb(df, SHUFFLE_SEED)
    print(f"[A.5-EMB] embeddings shuffled (seed={SHUFFLE_SEED})")

    total = confirmed = 0
    rows = []
    for topic in available:
        for lag in LAGS:
            result = run_ensemble(df_shuf, topic, lag)
            total += 1
            confirmed += result
            rows.append({"topic": topic, "lag": lag, "confirmed": result})
            print(f"  {topic:16s} lag={lag:3d}  {'CONFIRMED' if result else 'rejected'}")

    rate = confirmed / total if total > 0 else 0.0
    print(f"\n[A.5-EMB] Total: {total}  Confirmed: {confirmed}  FPR: {rate:.1%}")

    if rate <= 0.05:
        verdict = "PASS — embeddings don't overfit on shuffled data"
    elif rate <= 0.10:
        verdict = "MARGINAL — FPR slightly above 5%, monitor"
    else:
        verdict = "FAIL — embeddings overfit, do NOT use for signal detection"
    print(f"[A.5-EMB] Verdict: {verdict}")

    # Save
    result_df = pd.DataFrame(rows)
    csv_path = RESULTS / "phase_a5_embedding_test.csv"
    md_path  = RESULTS / "phase_a5_embedding_test.md"
    result_df.to_csv(csv_path, index=False)
    lines = [
        "# Phase A.5 (Embedding) — Shuffle test on embedding features",
        "",
        f"Instrument: {INSTRUMENT}",
        f"Topics: {available}",
        f"Lags: {LAGS}",
        f"Shuffle seed: {SHUFFLE_SEED}",
        "",
        "## Result",
        "",
        f"**Confirmed (shuffled): {confirmed} / {total} = {rate:.1%}**",
        "",
        f"**Verdict: {verdict}**",
        "",
        "| Topic | Lag | Confirmed |",
        "|---|---|---|",
    ]
    for row in rows:
        lines.append(f"| {row['topic']} | {row['lag']} | {'YES' if row['confirmed'] else 'no'} |")
    md_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"[A.5-EMB] wrote: {csv_path}")
    print(f"[A.5-EMB] wrote: {md_path}")
    return rate


if __name__ == "__main__":
    main()
