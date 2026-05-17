"""Phase A.5 — Shuffle test on real Train data.

Tests whether the frozen ensemble (M5+M6/AND) gives ~5% false-positive
rate on real training news when news dates are randomly permuted.

Steps:
1. Load v_train_news + v_train_market_data (BRENT) + v_key_rate_daily
   from signal_mind.duckdb (read-only).
2. Join on date, compute log-returns for market.
3. Shuffle news topic columns independently with a fixed seed (null
   hypothesis: news has no predictive power by construction).
4. Run the ensemble on all topic × lag hypothesis combinations.
5. Report confirmed rate. Expected: ~5% (alpha level).
6. If > 10%, M009 fires -> update catalog, return to A.3b.

Run:
    python -m analytics.testbed.ensemble.phase_a5_shuffle_test
"""
from __future__ import annotations

import io
import sys
from pathlib import Path

import numpy as np
import pandas as pd

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                  line_buffering=True)

HERE = Path(__file__).resolve().parent
ROOT_TESTBED = HERE.parent
ROOT_PROJECT = ROOT_TESTBED.parents[1]
RESULTS = ROOT_TESTBED / "results"
DB_PATH = ROOT_PROJECT / "db" / "signal_mind.duckdb"

SHUFFLE_SEED = 42
INSTRUMENT = "BRENT"   # liquid enough, in train window
TOPICS = ["oil", "rate", "ruble", "sanctions", "inflation", "banking", "gold"]
LAGS = [1, 7, 14, 30, 60, 90]


# ---------------------------------------------------------------------------
# Load real data
# ---------------------------------------------------------------------------

def load_real_data() -> pd.DataFrame:
    """Load and join news + market + key_rate. Returns daily DataFrame."""
    try:
        import duckdb
    except ImportError:
        raise RuntimeError("duckdb not installed")

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        news_df = con.execute("SELECT * FROM v_train_news").fetchdf()
        news_df = news_df.rename(columns={"news_date": "date"})
        news_df["date"] = pd.to_datetime(news_df["date"])

        market_q = (
            "SELECT trade_date AS date, close "
            "FROM v_train_market_data "
            f"WHERE instrument = '{INSTRUMENT}' "
            "ORDER BY trade_date"
        )
        market_df = con.execute(market_q).fetchdf()
        market_df["date"] = pd.to_datetime(market_df["date"])
        market_df = market_df.sort_values("date").reset_index(drop=True)
        market_df["market_return"] = np.log(market_df["close"]).diff()

        kr_df = con.execute(
            "SELECT period_date AS date, rate_pct AS key_rate_pct "
            "FROM v_key_rate_daily ORDER BY period_date"
        ).fetchdf()
        kr_df["date"] = pd.to_datetime(kr_df["date"])
        # Forward-fill key rate to daily
        # Merge on nearest date (market dates)
        market_df = pd.merge_asof(
            market_df.sort_values("date"),
            kr_df.sort_values("date"),
            on="date", direction="backward",
        )

        # Outer join: use market dates as spine
        df = pd.merge(market_df, news_df, on="date", how="inner")
        df = df.sort_values("date").reset_index(drop=True)
    finally:
        con.close()

    return df


# ---------------------------------------------------------------------------
# Build shuffled dataset
# ---------------------------------------------------------------------------

def shuffle_news(df: pd.DataFrame, seed: int) -> pd.DataFrame:
    """Permute news topic columns independently (null hypothesis)."""
    rng = np.random.default_rng(seed)
    df_shuf = df.copy()
    for col in TOPICS:
        if col in df_shuf.columns:
            df_shuf[col] = rng.permutation(df_shuf[col].to_numpy())
    return df_shuf


# ---------------------------------------------------------------------------
# Run ensemble on one hypothesis
# ---------------------------------------------------------------------------

def run_ensemble(df: pd.DataFrame, topic: str, lag: int) -> bool:
    """Apply M5+M6 ensemble (AND) to one hypothesis. Returns True if confirmed."""
    import importlib
    from analytics.testbed.methods.base import Hypothesis

    hyp = Hypothesis(
        news_field=topic,
        target_field="market_return",
        lag_days=lag,
    )

    results: list[bool] = []
    for mod_name in ("analytics.testbed.methods.m5_var",
                     "analytics.testbed.methods.m6_lgbm"):
        mod = importlib.import_module(mod_name)
        method = mod.build()
        verdict = method.evaluate(df, hyp)
        results.append(verdict.confirmed)

    return all(results)  # AND rule


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print(f"[A.5] loading real train data from {DB_PATH} ...")
    df = load_real_data()
    print(f"[A.5] real data shape: {df.shape}")
    print(f"[A.5] date range: {df['date'].min().date()} to {df['date'].max().date()}")
    topics_available = [t for t in TOPICS if t in df.columns]
    print(f"[A.5] topics: {topics_available}")

    # Shuffle
    df_shuf = shuffle_news(df, SHUFFLE_SEED)
    print(f"[A.5] news shuffled (seed={SHUFFLE_SEED})")

    # Run ensemble on all topic × lag combos
    total = 0
    confirmed = 0
    rows: list[dict] = []
    for topic in topics_available:
        for lag in LAGS:
            result = run_ensemble(df_shuf, topic, lag)
            total += 1
            if result:
                confirmed += 1
            rows.append({
                "topic": topic, "lag": lag,
                "confirmed": result,
            })
            status = "CONFIRMED" if result else "rejected"
            print(f"  {topic:12s} lag={lag:3d}  {status}")

    rate = confirmed / total if total > 0 else 0.0
    print()
    print(f"[A.5] Total hypotheses: {total}")
    print(f"[A.5] Confirmed (shuffled): {confirmed} ({rate:.1%})")
    print()

    if rate <= 0.05:
        verdict = "PASS — FPR within expected ~5% alpha level"
    elif rate <= 0.10:
        verdict = "MARGINAL — FPR slightly above 5%, acceptable but monitor"
    else:
        verdict = "FAIL — FPR > 10%, M009 fires, return to A.3b"
    print(f"[A.5] Verdict: {verdict}")

    # Save results
    result_df = pd.DataFrame(rows)
    csv_path = RESULTS / "phase_a5_shuffle_test.csv"
    md_path = RESULTS / "phase_a5_shuffle_test.md"
    result_df.to_csv(csv_path, index=False)

    lines = [
        "# Phase A.5 — Shuffle test on real Train data",
        "",
        f"Instrument: {INSTRUMENT}",
        f"Topics: {topics_available}",
        f"Lags: {LAGS}",
        f"Shuffle seed: {SHUFFLE_SEED}",
        f"Total hypotheses: {total}",
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
        lines.append(f"| {row['topic']} | {row['lag']} | "
                     f"{'YES' if row['confirmed'] else 'no'} |")
    lines += ["", f"Raw: `{csv_path.name}`"]
    md_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"[A.5] wrote: {csv_path}")
    print(f"[A.5] wrote: {md_path}")
    return rate


if __name__ == "__main__":
    main()
