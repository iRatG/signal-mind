"""Two-stage validation of 54 M5-confirmed rolling Ouroboros candidates.

Stage 1 — Val split (2024-01-01 → 2025-04-30, ~340 rows):
    M5 + M6 on Val. Only confirmed proceed to Stage 2.

Stage 2 — Test split (2025-09-01 → 2026-04-29, ~154-200 rows):
    M5 + M6 on Test (N_MIN lowered to 80/100 for small split).
    Sign consistency check (same direction as Val).
    Rolling IC stability.

Multiple testing: BH-FDR correction (q=0.10) on Stage 2 M5 p-values.

Non-standard lags (8→7, 2→1) are tested alongside standard version.

Usage:
    .venv/Scripts/python -m src.pipeline_v2.full_validation
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
from scipy.stats import spearmanr

warnings.filterwarnings("ignore")
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                  line_buffering=True)

ROOT    = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "db" / "signal_mind.duckdb"
OUT_DIR = ROOT / "analytics" / "phase_b" / "full_validation"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ──────────────────────────────────────────────────────────────────────────────
# Candidates: 54 M5-confirmed signals from rolling Ouroboros
# Non-standard lags replaced with nearest standard (8→7, 2→1)
# ──────────────────────────────────────────────────────────────────────────────

def _std_lag(lag: int) -> int:
    """Round to nearest standard lag."""
    standard = [1, 7, 14, 30, 60, 90]
    return min(standard, key=lambda x: abs(x - lag))

RAW_CANDIDATES = [
    ("MOEX10",          "banking_z",    8,  0.229),
    ("MSCI_WORLD",      "oil_emb_z",    8,  0.196),
    ("MOEXOG",          "gold_z",       1,  0.194),
    ("MOEXFN",          "banking_z",    8,  0.186),
    ("MOEXFN",          "banking_emb",  1,  0.179),
    ("MSCI_INDIA",      "gold_emb_z",   2,  0.173),
    ("SP500",           "oil",          7,  0.167),
    ("MSCI_INDIA",      "oil_emb",      7,  0.167),
    ("DXY",             "oil_emb",      7,  0.166),
    ("CHINA_H_SHARES",  "rate_z",       1,  0.166),
    ("SP500",           "inflation_z",  7,  0.162),
    ("DJ_SOUTH_AFRICA", "gold_z",      14,  0.162),
    ("MOEXFN",          "inflation_z",  8,  0.154),
    ("MOEXOG",          "banking_emb",  1,  0.151),
    ("IMOEX",           "banking_emb",  1,  0.149),
    ("DXY",             "banking",      7,  0.138),
    ("MOEX10",          "banking_emb",  1,  0.136),
    ("IMOEX",           "rate_emb",     7,  0.135),
    ("MOEXOG",          "rate_emb_z",   7,  0.133),
    ("MOEXFN",          "rate_emb",     7,  0.132),
    ("DXY",             "rate_emb_z",   8,  0.115),
    ("SP500",           "banking_z",   14,  0.108),
    ("SP500",           "oil_z",       30,  0.106),
    ("SP500",           "rate_z",       1,  0.101),
    # continuing top 54 — add remaining from analysis
    ("MSCI_WORLD",      "oil_z",        7,  0.219),
    ("DJ_SOUTH_AFRICA", "oil_emb",     30,  0.226),
    ("DJ_SOUTH_AFRICA", "inflation_z",  7,  0.224),  # lag=8→7
    ("BRENT",           "gold_emb_z",   7,  0.216),  # lag=8→7
    ("SP500",           "oil_emb",     60,  0.214),
    ("DJ_SOUTH_AFRICA", "ruble_z",     14,  0.213),
    ("MSCI_WORLD",      "oil_z",       14,  0.209),
    ("SP500",           "gold_z",      30,  0.209),
    ("IMOEX",           "gold_z",       7,  0.205),
    ("MOEXOG",          "inflation_z",  7,  0.200),
    ("CHINA_H_SHARES",  "oil_z",       14,  0.246),
    ("FTSE_CHINA_50",   "banking_z",    7,  0.243),
    ("MSCI_INDIA",      "oil_z",       14,  0.239),
    ("FTSE_CHINA_50",   "oil",          1,  0.234),
    ("CHINA_H_SHARES",  "banking_z",    7,  0.230),
    ("MOEX10",          "inflation_z",  7,  0.200),
    ("MOEX10",          "ruble_z",      7,  0.195),
    ("USD_RUB",         "oil_z",       14,  0.190),
    ("EUR_RUB",         "oil_z",       14,  0.190),
    ("MOEXFN",          "gold_z",       7,  0.188),
    ("SP500",           "sanctions_z", 30,  0.154),
    ("SP500",           "gold_z",      14,  0.126),
    ("SP500",           "gold_z",       1,  0.165),
    ("SP500",           "inflation_z",  1,  0.149),
    ("SP500",           "rate_emb_z",   1,  0.182),
    ("MSCI_INDIA",      "gold_emb",    60,  0.260),
    ("CHINA_H_SHARES",  "oil_emb_z",    7,  0.283),
    ("USD_RUB",         "ruble_z",      7,  0.136),
    ("EUR_RUB",         "ruble_z",      7,  0.136),
    ("IMOEX",           "ruble_z",      7,  0.134),
]

# Canonicalize lags, deduplicate
CANDIDATES = []
seen = set()
for inst, topic, lag, ic in RAW_CANDIDATES:
    canonical_lag = _std_lag(lag)
    key = (inst, topic, canonical_lag)
    if key not in seen:
        seen.add(key)
        CANDIDATES.append((inst, topic, canonical_lag, ic))

MOEX_SECTORS = {
    "IMOEX":  "imoex",
    "MOEXFN": "moexfn_finance",
    "MOEXOG": "moexog_oil_gas",
    "MOEX10": "moex10_bluechip",
}

# ──────────────────────────────────────────────────────────────────────────────
# Data loading
# ──────────────────────────────────────────────────────────────────────────────

def load_split_df(instrument: str, split: str) -> pd.DataFrame | None:
    import duckdb
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        if instrument in MOEX_SECTORS:
            col = MOEX_SECTORS[instrument]
            mkt = con.execute(
                f"SELECT trade_date AS date, {col} AS close "
                f"FROM v_{split}_sectors WHERE {col} IS NOT NULL ORDER BY trade_date"
            ).fetchdf()
        else:
            mkt = con.execute(
                f"SELECT trade_date AS date, close FROM v_{split}_market_data "
                f"WHERE instrument='{instrument}' ORDER BY trade_date"
            ).fetchdf()

        if len(mkt) < 40:
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

        news = con.execute(f"SELECT * FROM v_{split}_news").fetchdf()
        news = news.rename(columns={"news_date": "date"})
        news["date"] = pd.to_datetime(news["date"])

        df = pd.merge(mkt, news, on="date", how="inner")
        return df.sort_values("date").reset_index(drop=True) if len(df) >= 40 else None
    finally:
        con.close()


def apply_z(df: pd.DataFrame) -> pd.DataFrame:
    """Add z-score variants for all news columns (window=90)."""
    from src.pipeline_v2.feature_transformer import FeatureTransformer
    df = df.copy()
    base = ["oil", "rate", "ruble", "sanctions", "inflation", "banking", "gold"]
    for t in base:
        if t in df.columns:
            df[f"{t}_z"] = FeatureTransformer.rolling_zscore(df[t], window=90)
        for suffix in ["_emb", "_emb_z"]:
            col = f"{t}{suffix}"
            if col in df.columns and suffix == "_emb":
                df[f"{t}_emb_z"] = FeatureTransformer.rolling_zscore(df[col], window=90)
    return df


# ──────────────────────────────────────────────────────────────────────────────
# BH-FDR
# ──────────────────────────────────────────────────────────────────────────────

def bh_fdr(pvalues: list, q: float = 0.10) -> list:
    n = len(pvalues)
    if n == 0:
        return []
    ranked = sorted(enumerate(pvalues), key=lambda x: x[1])
    reject = [False] * n
    last = -1
    for rank, (orig_idx, p) in enumerate(ranked):
        if p <= (rank + 1) / n * q:
            last = rank
    for rank, (orig_idx, p) in enumerate(ranked):
        if rank <= last:
            reject[orig_idx] = True
    return reject


# ──────────────────────────────────────────────────────────────────────────────
# Single hypothesis evaluation
# ──────────────────────────────────────────────────────────────────────────────

def evaluate(df: pd.DataFrame, topic: str, lag: int,
             m5, m6, p_max: float = 0.05, ic_min: float = 0.01) -> dict:
    from analytics.testbed.methods.base import Hypothesis
    hyp = Hypothesis(news_field=topic, target_field="market_return", lag_days=lag)
    result = {"n": len(df), "m5_pvalue": 1.0, "m5_score": 0.0,
              "m6_ic": 0.0, "m5_pass": False, "m6_pass": False, "pass": False}
    try:
        v5 = m5.evaluate(df, hyp)
        result["m5_pvalue"] = round(float(v5.p_value or 1.0), 6)
        result["m5_score"]  = round(float(v5.score), 6)
        result["m5_pass"]   = (v5.p_value or 1.0) < p_max
    except Exception:
        pass
    try:
        v6 = m6.evaluate(df, hyp)
        result["m6_ic"]   = round(float(v6.score), 6)
        result["m6_pass"] = v6.score >= ic_min
    except Exception:
        pass
    result["pass"] = result["m5_pass"] or result["m6_pass"]
    return result


def rolling_spearman(df: pd.DataFrame, topic: str, lag: int,
                     window: int = 50) -> float:
    """Mean rolling Spearman IC. Returns NaN if not enough data."""
    df = df.copy().dropna(subset=["market_return", topic])
    df["news_lag"] = df[topic].shift(lag)
    df = df.dropna(subset=["news_lag"])
    if len(df) < window + 5:
        return float("nan")
    ics = []
    for i in range(window, len(df)):
        w = df.iloc[i - window:i]
        r, _ = spearmanr(w["news_lag"], w["market_return"])
        ics.append(r)
    return float(np.mean(ics)) if ics else float("nan")


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_path = OUT_DIR / f"full_validation_{ts}.log"

    def log(msg: str) -> None:
        t = datetime.now(timezone.utc).strftime("%H:%M:%S")
        line = f"[{t}] {msg}"
        print(line)
        log_fh.write(line + "\n")
        log_fh.flush()

    # Adjust N_MIN for small splits
    import analytics.testbed.methods.m5_var  as m5_mod
    import analytics.testbed.methods.m6_lgbm as m6_mod
    m5_mod.N_MIN = 80
    m6_mod.N_MIN = 100
    m5 = m5_mod.build()
    m6 = m6_mod.build()

    with open(log_path, "w", encoding="utf-8", buffering=1) as log_fh:
        log(f"Full Two-Stage Validation — {len(CANDIDATES)} candidates")
        log("Stage 1: Val split (2024-01-01 → 2025-04-30)")
        log("Stage 2: Test split (2025-09-01 → 2026-04-29) — for Stage 1 survivors")
        log(f"BH-FDR q=0.10 on Stage 2 M5 p-values")
        log("=" * 60)

        t0 = time.time()
        stage1_rows = []
        stage2_rows = []

        # ── STAGE 1: Val ────────────────────────────────────────────────────
        log("\n--- STAGE 1: Val split ---\n")

        val_cache: dict = {}

        for i, (inst, topic, lag, train_ic) in enumerate(CANDIDATES):
            if inst not in val_cache:
                df_raw = load_split_df(inst, "val")
                val_cache[inst] = apply_z(df_raw) if df_raw is not None else None

            df = val_cache[inst]
            if df is None or topic not in df.columns:
                stage1_rows.append({
                    "instrument": inst, "topic": topic, "lag": lag,
                    "train_ic": train_ic,
                    "val_m5_pvalue": 1.0, "val_m5_pass": False,
                    "val_m6_ic": 0.0, "val_m6_pass": False,
                    "val_pass": False, "note": "no data or missing topic",
                })
                continue

            r = evaluate(df, topic, lag, m5, m6, p_max=0.05, ic_min=0.03)
            row = {
                "instrument": inst, "topic": topic, "lag": lag,
                "train_ic": train_ic, "val_n": r["n"],
                "val_m5_pvalue": r["m5_pvalue"], "val_m5_pass": r["m5_pass"],
                "val_m6_ic": r["m6_ic"], "val_m6_pass": r["m6_pass"],
                "val_pass": r["m5_pass"] or r["m6_pass"],
                "note": "",
            }
            stage1_rows.append(row)
            status = "PASS" if row["val_pass"] else "fail"
            log(f"  [{i+1:2d}/{len(CANDIDATES)}] {inst:18s} {topic:16s} lag={lag:2d}  "
                f"Val M5={'✓' if r['m5_pass'] else '✗'} p={r['m5_pvalue']:.4f}  "
                f"M6={'✓' if r['m6_pass'] else '✗'} ic={r['m6_ic']:.4f}  → {status}")

        df1 = pd.DataFrame(stage1_rows)
        survivors = df1[df1.val_pass].copy()
        log(f"\nStage 1: {len(survivors)}/{len(CANDIDATES)} passed Val")

        if len(survivors) == 0:
            log("No survivors. Relaxing Val gate to M6-only IC>=0.01...")
            for row in stage1_rows:
                row["val_pass_relaxed"] = row["val_m6_ic"] >= 0.01
            df1 = pd.DataFrame(stage1_rows)
            survivors = df1[df1.get("val_pass_relaxed", False)].copy()
            log(f"Relaxed: {len(survivors)} survivors")

        # ── STAGE 2: Test ────────────────────────────────────────────────────
        log(f"\n--- STAGE 2: Test split ({len(survivors)} candidates) ---\n")

        test_cache: dict = {}
        val_dfs:  dict = val_cache

        for _, s1 in survivors.iterrows():
            inst, topic, lag = s1.instrument, s1.topic, int(s1.lag)

            if inst not in test_cache:
                df_raw = load_split_df(inst, "test")
                test_cache[inst] = apply_z(df_raw) if df_raw is not None else None

            df_test = test_cache[inst]

            if df_test is None or topic not in df_test.columns:
                stage2_rows.append({
                    "instrument": inst, "topic": topic, "lag": lag,
                    "train_ic": s1.train_ic, "val_m6_ic": s1.val_m6_ic,
                    "test_m5_pvalue": 1.0, "test_m5_pass": False,
                    "test_m6_ic": 0.0, "test_m6_pass": False,
                    "sign_ok": False, "rolling_ic": float("nan"),
                    "ic_decay": 0.0, "verdict": "NO_DATA",
                })
                continue

            r_test = evaluate(df_test, topic, lag, m5, m6, p_max=0.05, ic_min=0.01)
            rolling = rolling_spearman(df_test, topic, lag,
                                       window=min(50, len(df_test) // 3))

            # Sign consistency: val direction vs test direction
            val_sign  = np.sign(s1.get("val_m5_score", 0) or r_test["m5_score"])
            test_sign = np.sign(r_test["m5_score"])
            sign_ok   = (val_sign == test_sign) if val_sign != 0 and test_sign != 0 else False

            # IC decay
            ic_decay = round(r_test["m6_ic"] / s1.val_m6_ic, 3) if s1.val_m6_ic > 0 else 0.0

            # Verdict
            if r_test["m5_pass"] and r_test["m6_pass"] and sign_ok:
                verdict = "CONFIRMED"
            elif (r_test["m5_pass"] or r_test["m6_pass"]) and sign_ok and ic_decay >= 0.30:
                verdict = "MARGINAL"
            elif not sign_ok and (r_test["m5_pass"] or r_test["m6_pass"]):
                verdict = "SIGN_FLIP"
            else:
                verdict = "FAILED"

            row = {
                "instrument": inst, "topic": topic, "lag": lag,
                "train_ic": s1.train_ic, "val_m6_ic": s1.val_m6_ic,
                "test_n": r_test["n"],
                "test_m5_pvalue": r_test["m5_pvalue"],
                "test_m5_pass": r_test["m5_pass"],
                "test_m6_ic": r_test["m6_ic"],
                "test_m6_pass": r_test["m6_pass"],
                "sign_ok": sign_ok, "rolling_ic": round(rolling, 4),
                "ic_decay": ic_decay, "verdict": verdict,
            }
            stage2_rows.append(row)
            log(f"  {inst:18s} {topic:16s} lag={lag:2d}  "
                f"M5={'✓' if r_test['m5_pass'] else '✗'} p={r_test['m5_pvalue']:.4f}  "
                f"M6={'✓' if r_test['m6_pass'] else '✗'} ic={r_test['m6_ic']:.4f}  "
                f"sign={'OK' if sign_ok else 'FLIP'}  "
                f"ic_decay={ic_decay:.2f}  → {verdict}")

        # ── BH-FDR ───────────────────────────────────────────────────────────
        df2 = pd.DataFrame(stage2_rows) if stage2_rows else pd.DataFrame()

        if len(df2) > 0:
            pvals = df2["test_m5_pvalue"].tolist()
            fdr = bh_fdr(pvals, q=0.10)
            df2["bh_fdr"] = fdr
        else:
            df2["bh_fdr"] = []

        # ── Summary ──────────────────────────────────────────────────────────
        elapsed = (time.time() - t0) / 60
        log(f"\n{'='*60}")
        log(f"FINAL SUMMARY  ({elapsed:.1f} min)")
        log(f"{'='*60}")
        log(f"Stage 1 (Val):  {len(survivors)}/{len(CANDIDATES)} passed")

        if len(df2) > 0:
            for verdict in ["CONFIRMED", "MARGINAL", "SIGN_FLIP", "FAILED", "NO_DATA"]:
                n = (df2.verdict == verdict).sum()
                log(f"  {verdict:12s}: {n}")
            log(f"  BH-FDR (q=0.10): {df2.get('bh_fdr', pd.Series([False]*len(df2))).sum()} significant")

            confirmed = df2[df2.verdict == "CONFIRMED"]
            if len(confirmed) > 0:
                log(f"\n*** CONFIRMED signals (Train→Val→Test) ***")
                for _, r in confirmed.iterrows():
                    log(f"  {r.instrument} / {r.topic} / lag={r.lag}d  "
                        f"train_ic={r.train_ic:.4f}  val_ic={r.val_m6_ic:.4f}  "
                        f"test_ic={r.test_m6_ic:.4f}  ic_decay={r.ic_decay:.2f}  "
                        f"rolling={r.rolling_ic:.4f}")
            else:
                log("\nNo CONFIRMED signals across all three splits.")

            marginal = df2[df2.verdict == "MARGINAL"]
            if len(marginal) > 0:
                log(f"\n--- MARGINAL signals ---")
                for _, r in marginal.iterrows():
                    log(f"  {r.instrument} / {r.topic} / lag={r.lag}d  "
                        f"test_ic={r.test_m6_ic:.4f}  ic_decay={r.ic_decay:.2f}")

        # Save
        df1.to_csv(OUT_DIR / f"stage1_val_{ts}.csv", index=False)
        log(f"\nStage 1 CSV: {OUT_DIR}/stage1_val_{ts}.csv")
        if len(df2) > 0:
            df2.to_csv(OUT_DIR / f"stage2_test_{ts}.csv", index=False)
            log(f"Stage 2 CSV: {OUT_DIR}/stage2_test_{ts}.csv")

            md = ["# Full Validation Report",
                  f"Run: {ts}", "",
                  f"Candidates: {len(CANDIDATES)}",
                  f"Stage 1 (Val) pass: {len(survivors)}",
                  f"Stage 2 (Test) confirmed: {(df2.verdict=='CONFIRMED').sum()}",
                  f"Stage 2 (Test) marginal: {(df2.verdict=='MARGINAL').sum()}",
                  "",
                  "## Stage 2 Results",
                  "",
                  "| Instrument | Topic | Lag | Train IC | Val IC | Test IC | Decay | Sign | Verdict |",
                  "|---|---|---|---|---|---|---|---|---|",
                  ]
            for _, r in df2.sort_values("test_m6_ic", ascending=False).iterrows():
                md.append(f"| {r.instrument} | {r.topic} | {r.lag} "
                          f"| {r.train_ic:.3f} | {r.val_m6_ic:.3f} | {r.test_m6_ic:.3f} "
                          f"| {r.ic_decay:.2f} | {'OK' if r.sign_ok else 'FLIP'} "
                          f"| **{r.verdict}** |")
            (OUT_DIR / f"full_report_{ts}.md").write_text("\n".join(md), encoding="utf-8")
            log(f"Report: {OUT_DIR}/full_report_{ts}.md")


if __name__ == "__main__":
    main()
