"""Deep Test-Split Validation of Gold Signals.

Takes the 21 M5+Val confirmed signals from Ouroboros and does a thorough
test on the held-out Test split (2025-09-01 to 2026-04-29):

  1. M5 (VAR/IRF): structural causality on Test
  2. M6 (LightGBM): walk-forward IC on Test
  3. Sign consistency: same direction as Train?
  4. Rolling IC: is Test IC stable or a single spike?
  5. BH-FDR correction: multiple testing adjustment (21 hypotheses)
  6. Verdict: CONFIRMED / MARGINAL / FAILED

Usage:
    .venv/Scripts/python -m src.pipeline_v2.test_validation
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
from scipy import stats

warnings.filterwarnings("ignore")

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                  line_buffering=True)

ROOT    = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "db" / "signal_mind.duckdb"
OUT_DIR = ROOT / "analytics" / "phase_b" / "test_validation"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ──────────────────────────────────────────────────────────────────────────────
# Gold signals from Ouroboros (21 candidates)
# ──────────────────────────────────────────────────────────────────────────────

GOLD_SIGNALS = [
    # (instrument, topic, lag, train_ic, rounds)
    ("GOLD",            "inflation_z",      7,  0.138, 82),
    ("BRENT",           "sanctions",        7,  0.122,  3),
    ("MSCI_WORLD",      "sanctions_z",     14,  0.099, 65),
    ("FTSE_CHINA_50",   "rate_z",           1,  0.094,  6),
    ("DJ_SOUTH_AFRICA", "sanctions_z",     14,  0.091,  7),
    ("CHINA_H_SHARES",  "rate_z",           1,  0.081,  6),
    ("DXY",             "oil_z",            1,  0.079,  6),
    ("FTSE_CHINA_50",   "rate_emb",        14,  0.078,  3),
    ("CHINA_H_SHARES",  "rate_emb",        14,  0.077,  3),
    ("DXY",             "rate_emb",        14,  0.077,  3),
    ("CHINA_H_SHARES",  "ruble_emb",        7,  0.072,  3),
    ("MSCI_INDIA",      "inflation_emb_z", 14,  0.070,  3),
    ("MOEXFN",          "sanctions_z",      1,  0.070,  6),
    ("IMOEX",           "ruble_z",          7,  0.069,  6),
    ("FTSE_CHINA_50",   "inflation_emb",   14,  0.068,  3),
    ("DJ_SOUTH_AFRICA", "sanctions_emb_z",  7,  0.067,  3),
    ("DJ_SOUTH_AFRICA", "sanctions_emb",    7,  0.061,  3),
    ("MOEXOG",          "rate_emb",         1,  0.049,  3),
    ("CHINA_H_SHARES",  "rate_emb_z",      14,  0.040,  3),
    ("DJ_SOUTH_AFRICA", "sanctions",       14,  0.038,  3),
    ("DJ_SOUTH_AFRICA", "gold",             7,  0.038,  3),
]

MOEX_SECTORS = {
    "IMOEX":  "imoex",
    "MOEXFN": "moexfn_finance",
    "MOEXOG": "moexog_oil_gas",
    "MOEX10": "moex10_bluechip",
}

# ──────────────────────────────────────────────────────────────────────────────
# Data loading
# ──────────────────────────────────────────────────────────────────────────────

def load_test_df(instrument: str) -> pd.DataFrame | None:
    """Load test split for one instrument, with all news features."""
    import duckdb
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        # Market data
        if instrument in MOEX_SECTORS:
            col = MOEX_SECTORS[instrument]
            mkt = con.execute(
                f"SELECT trade_date AS date, {col} AS close "
                f"FROM v_test_sectors WHERE {col} IS NOT NULL ORDER BY trade_date"
            ).fetchdf()
        else:
            mkt = con.execute(
                f"SELECT trade_date AS date, close FROM v_test_market_data "
                f"WHERE instrument='{instrument}' ORDER BY trade_date"
            ).fetchdf()

        if len(mkt) < 50:
            return None

        mkt["date"] = pd.to_datetime(mkt["date"])
        mkt = mkt.sort_values("date").reset_index(drop=True)
        mkt["market_return"] = np.log(mkt["close"]).diff()

        # Key rate
        kr = con.execute(
            "SELECT period_date AS date, rate_pct AS key_rate_pct "
            "FROM v_key_rate_daily ORDER BY period_date"
        ).fetchdf()
        kr["date"] = pd.to_datetime(kr["date"])
        mkt = pd.merge_asof(mkt.sort_values("date"), kr.sort_values("date"),
                            on="date", direction="backward")

        # News (all columns)
        news = con.execute("SELECT * FROM v_test_news").fetchdf()
        news = news.rename(columns={"news_date": "date"})
        news["date"] = pd.to_datetime(news["date"])

        df = pd.merge(mkt, news, on="date", how="inner")
        return df.sort_values("date").reset_index(drop=True) if len(df) >= 50 else None
    finally:
        con.close()


def load_train_df(instrument: str) -> pd.DataFrame | None:
    """Load train split (for sign/direction comparison)."""
    import duckdb
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        if instrument in MOEX_SECTORS:
            col = MOEX_SECTORS[instrument]
            mkt = con.execute(
                f"SELECT trade_date AS date, {col} AS close "
                f"FROM v_train_sectors WHERE {col} IS NOT NULL ORDER BY trade_date"
            ).fetchdf()
        else:
            mkt = con.execute(
                f"SELECT trade_date AS date, close FROM v_train_market_data "
                f"WHERE instrument='{instrument}' ORDER BY trade_date"
            ).fetchdf()

        if len(mkt) < 50:
            return None
        mkt["date"] = pd.to_datetime(mkt["date"])
        mkt = mkt.sort_values("date").reset_index(drop=True)
        mkt["market_return"] = np.log(mkt["close"]).diff()

        kr = con.execute(
            "SELECT period_date AS date, rate_pct AS key_rate_pct "
            "FROM v_key_rate_daily ORDER BY period_date"
        ).fetchdf()
        kr["date"] = pd.to_datetime(kr["date"])
        mkt = pd.merge_asof(mkt.sort_values("date"), kr.sort_values("date"),
                            on="date", direction="backward")

        news = con.execute("SELECT * FROM v_train_news").fetchdf()
        news = news.rename(columns={"news_date": "date"})
        news["date"] = pd.to_datetime(news["date"])

        df = pd.merge(mkt, news, on="date", how="inner")
        return df.sort_values("date").reset_index(drop=True) if len(df) >= 50 else None
    finally:
        con.close()


# ──────────────────────────────────────────────────────────────────────────────
# Feature transform (same as Ouroboros)
# ──────────────────────────────────────────────────────────────────────────────

def apply_features(df: pd.DataFrame, topic: str) -> pd.DataFrame:
    """Add z-score and embedding_z variants of the topic column."""
    from src.pipeline_v2.feature_transformer import FeatureTransformer
    df = df.copy()

    base_topics = ["oil", "rate", "ruble", "sanctions", "inflation", "banking", "gold"]
    for t in base_topics:
        if t in df.columns:
            df[f"{t}_z"] = FeatureTransformer.rolling_zscore(df[t], window=90)
        if f"{t}_emb" in df.columns:
            df[f"{t}_emb_z"] = FeatureTransformer.rolling_zscore(df[f"{t}_emb"], window=90)

    return df


# ──────────────────────────────────────────────────────────────────────────────
# Deep validation of one signal
# ──────────────────────────────────────────────────────────────────────────────

def rolling_ic(df: pd.DataFrame, topic: str, lag: int,
               window: int = 60) -> pd.Series:
    """Rolling Spearman IC over Test period (window=60 trading days)."""
    from scipy.stats import spearmanr
    df = df.copy().dropna(subset=["market_return", topic])
    df[f"news_lag"] = df[topic].shift(lag)
    df = df.dropna(subset=["news_lag", "market_return"])
    if len(df) < window + 10:
        return pd.Series(dtype=float)

    ics = []
    dates = []
    for i in range(window, len(df)):
        window_df = df.iloc[i - window:i]
        r, _ = spearmanr(window_df["news_lag"], window_df["market_return"])
        ics.append(r)
        dates.append(df.iloc[i]["date"])
    return pd.Series(ics, index=dates)


def validate_signal(instrument: str, topic: str, lag: int,
                    train_ic: float, rounds: int,
                    m5, m6, log_fn) -> dict:
    """Full deep validation of one signal on Test split."""
    from analytics.testbed.methods.base import Hypothesis

    result = {
        "instrument": instrument, "topic": topic, "lag": lag,
        "train_ic": train_ic, "rounds": rounds,
        "n_test": 0,
        "m5_confirmed": False, "m5_pvalue": 1.0, "m5_score": 0.0,
        "m6_confirmed": False, "m6_ic": 0.0,
        "sign_consistent": False,
        "rolling_ic_mean": 0.0, "rolling_ic_std": 0.0, "rolling_ic_positive_pct": 0.0,
        "ic_degradation": 0.0,  # test_ic / train_ic
        "verdict": "FAILED",
        "note": "",
    }

    # Load test data
    df_test = load_test_df(instrument)
    if df_test is None:
        result["note"] = "no test data"
        return result

    df_test = apply_features(df_test, topic)

    if topic not in df_test.columns:
        result["note"] = f"topic '{topic}' not in test data"
        return result

    result["n_test"] = len(df_test)

    hyp = Hypothesis(news_field=topic, target_field="market_return", lag_days=lag)

    # ── M5 on Test ───────────────────────────────────────────────────────────
    try:
        v5 = m5.evaluate(df_test, hyp)
        result["m5_pvalue"] = round(float(v5.p_value or 1.0), 6)
        result["m5_score"]  = round(float(v5.score), 6)
        result["m5_confirmed"] = v5.p_value is not None and v5.p_value < 0.05
    except Exception as e:
        result["note"] += f"M5 error: {e}; "

    # ── M6 on Test ───────────────────────────────────────────────────────────
    try:
        v6 = m6.evaluate(df_test, hyp)
        result["m6_ic"] = round(float(v6.score), 6)
        result["m6_confirmed"] = v6.score >= 0.03
    except Exception as e:
        result["note"] += f"M6 error: {e}; "

    # ── Sign consistency (Train direction vs Test direction) ─────────────────
    df_train = load_train_df(instrument)
    if df_train is not None:
        df_train = apply_features(df_train, topic)
        if topic in df_train.columns:
            try:
                v5_tr = m5.evaluate(df_train, hyp)
                result["sign_consistent"] = (
                    np.sign(v5_tr.score) == np.sign(result["m5_score"])
                    if result["m5_score"] != 0 else False
                )
            except Exception:
                pass

    # ── Rolling IC on Test ───────────────────────────────────────────────────
    try:
        ric = rolling_ic(df_test, topic, lag, window=min(60, len(df_test) // 3))
        if len(ric) > 3:
            result["rolling_ic_mean"] = round(float(ric.mean()), 6)
            result["rolling_ic_std"]  = round(float(ric.std()), 6)
            result["rolling_ic_positive_pct"] = round(float((ric > 0).mean()), 4)
    except Exception as e:
        result["note"] += f"RollingIC error: {e}; "

    # ── IC degradation: test_ic / train_ic ───────────────────────────────────
    if train_ic > 0:
        result["ic_degradation"] = round(result["m6_ic"] / train_ic, 4)

    # ── Verdict ──────────────────────────────────────────────────────────────
    m5_ok  = result["m5_confirmed"]
    m6_ok  = result["m6_confirmed"]
    sign_ok = result["sign_consistent"]
    ric_ok = result["rolling_ic_positive_pct"] >= 0.50
    ic_deg = result["ic_degradation"]

    if m5_ok and m6_ok and sign_ok and ric_ok:
        result["verdict"] = "CONFIRMED"
    elif (m5_ok or m6_ok) and sign_ok and ic_deg >= 0.30:
        result["verdict"] = "MARGINAL"
    elif not sign_ok and (m5_ok or m6_ok):
        result["verdict"] = "SIGN_FLIP"  # dangerous: opposite direction on test
    else:
        result["verdict"] = "FAILED"

    return result


# ──────────────────────────────────────────────────────────────────────────────
# BH-FDR correction
# ──────────────────────────────────────────────────────────────────────────────

def bh_fdr(pvalues: list[float], q: float = 0.10) -> list[bool]:
    """Benjamini-Hochberg FDR correction. Returns list of rejection decisions."""
    n = len(pvalues)
    if n == 0:
        return []
    ranked = sorted(enumerate(pvalues), key=lambda x: x[1])
    reject = [False] * n
    for rank, (orig_idx, p) in enumerate(ranked):
        threshold = (rank + 1) / n * q
        if p <= threshold:
            reject[orig_idx] = True
    # BH: reject all up to the last rejection
    last_reject = -1
    for rank, (orig_idx, p) in enumerate(ranked):
        threshold = (rank + 1) / n * q
        if p <= threshold:
            last_reject = rank
    for rank, (orig_idx, p) in enumerate(ranked):
        if rank <= last_reject:
            reject[orig_idx] = True
    return reject


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_path = OUT_DIR / f"test_validation_{ts}.log"

    def log(msg: str) -> None:
        t = datetime.now(timezone.utc).strftime("%H:%M:%S")
        line = f"[{t}] {msg}"
        print(line)
        log_fh.write(line + "\n")
        log_fh.flush()

    m5 = importlib.import_module("analytics.testbed.methods.m5_var").build()
    m6 = importlib.import_module("analytics.testbed.methods.m6_lgbm").build()

    with open(log_path, "w", encoding="utf-8", buffering=1) as log_fh:
        log(f"Deep Test Validation — {len(GOLD_SIGNALS)} gold signals")
        log(f"Test split: 2025-09-01 → 2026-04-29 (held-out, never touched)")
        log(f"Checks: M5, M6, sign consistency, rolling IC, BH-FDR")
        log("=" * 60)

        results = []
        t0 = time.time()

        for i, (inst, topic, lag, train_ic, rounds) in enumerate(GOLD_SIGNALS):
            log(f"\n[{i+1}/{len(GOLD_SIGNALS)}] {inst} / {topic} / lag={lag}d  "
                f"(train_ic={train_ic:.4f}, rounds={rounds})")

            r = validate_signal(inst, topic, lag, train_ic, rounds, m5, m6, log)
            results.append(r)

            log(f"  M5: {'PASS' if r['m5_confirmed'] else 'fail'} (p={r['m5_pvalue']:.4f})  "
                f"M6: {'PASS' if r['m6_confirmed'] else 'fail'} (IC={r['m6_ic']:.4f})  "
                f"sign={'OK' if r['sign_consistent'] else 'FLIP'}  "
                f"rolling_ic={r['rolling_ic_mean']:.4f}±{r['rolling_ic_std']:.4f}  "
                f"IC_degr={r['ic_degradation']:.2f}  "
                f"→ {r['verdict']}")

        elapsed = time.time() - t0
        log(f"\n{'='*60}")
        log(f"Completed in {elapsed/60:.1f} min")

        # ── BH-FDR correction on M5 p-values ─────────────────────────────────
        pvals = [r["m5_pvalue"] for r in results]
        fdr_reject = bh_fdr(pvals, q=0.10)
        for r, rej in zip(results, fdr_reject):
            r["bh_fdr_reject"] = rej

        # ── Summary ──────────────────────────────────────────────────────────
        df = pd.DataFrame(results)
        confirmed  = df[df.verdict == "CONFIRMED"]
        marginal   = df[df.verdict == "MARGINAL"]
        sign_flip  = df[df.verdict == "SIGN_FLIP"]
        failed     = df[df.verdict == "FAILED"]

        log(f"\n{'='*60}")
        log("FINAL VERDICTS")
        log(f"{'='*60}")
        log(f"CONFIRMED:  {len(confirmed)}")
        log(f"MARGINAL:   {len(marginal)}")
        log(f"SIGN_FLIP:  {len(sign_flip)}  ← dangerous, opposite direction on test")
        log(f"FAILED:     {len(failed)}")
        log(f"BH-FDR significant (q<0.10): {sum(fdr_reject)}")

        if len(confirmed) > 0:
            log(f"\n*** CONFIRMED SIGNALS ***")
            for _, row in confirmed.iterrows():
                log(f"  {row.instrument} / {row.topic} / lag={row.lag}d  "
                    f"IC_test={row.m6_ic:.4f}  IC_degr={row.ic_degradation:.2f}  "
                    f"rolling_pos={row.rolling_ic_positive_pct:.0%}")

        if len(marginal) > 0:
            log(f"\n--- MARGINAL SIGNALS ---")
            for _, row in marginal.iterrows():
                log(f"  {row.instrument} / {row.topic} / lag={row.lag}d  "
                    f"IC_test={row.m6_ic:.4f}  IC_degr={row.ic_degradation:.2f}")

        if len(sign_flip) > 0:
            log(f"\n!!! SIGN FLIPS (direction reversed on test) !!!")
            for _, row in sign_flip.iterrows():
                log(f"  {row.instrument} / {row.topic} / lag={row.lag}d")

        # Save results
        csv_path = OUT_DIR / f"test_validation_{ts}.csv"
        df.to_csv(csv_path, index=False)
        log(f"\nCSV: {csv_path}")

        # Markdown report
        md_lines = [
            "# Test Split Validation — Gold Signals",
            f"Run: {ts}",
            f"Test split: 2025-09-01 → 2026-04-29",
            "",
            "## Summary",
            f"| Verdict | Count |",
            f"|---|---|",
            f"| CONFIRMED | {len(confirmed)} |",
            f"| MARGINAL  | {len(marginal)} |",
            f"| SIGN_FLIP | {len(sign_flip)} |",
            f"| FAILED    | {len(failed)} |",
            f"| BH-FDR significant (q<0.10) | {sum(fdr_reject)} |",
            "",
            "## Full Results",
            "",
            "| # | Instrument | Topic | Lag | Train IC | Test M5 p | Test M6 IC | "
            "IC_degr | Sign | Rolling IC | BH-FDR | Verdict |",
            "|---|---|---|---|---|---|---|---|---|---|---|---|",
        ]
        for i, row in df.iterrows():
            md_lines.append(
                f"| {i+1} | {row.instrument} | {row.topic} | {row.lag} "
                f"| {row.train_ic:.4f} | {row.m5_pvalue:.4f} | {row.m6_ic:.4f} "
                f"| {row.ic_degradation:.2f} | {'OK' if row.sign_consistent else 'FLIP'} "
                f"| {row.rolling_ic_mean:.4f}±{row.rolling_ic_std:.4f} "
                f"({row.rolling_ic_positive_pct:.0%}+) "
                f"| {'★' if row.bh_fdr_reject else '-'} "
                f"| **{row.verdict}** |"
            )

        md_path = OUT_DIR / f"test_validation_{ts}.md"
        md_path.write_text("\n".join(md_lines), encoding="utf-8")
        log(f"Report: {md_path}")


if __name__ == "__main__":
    main()
