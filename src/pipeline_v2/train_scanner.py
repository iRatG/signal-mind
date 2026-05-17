"""Phase B — Real Train run with frozen M5+M6/AND ensemble.

Scans ALL (instrument × topic × lag) combinations on real Train data,
immediately validates confirmed signals on Val data, and generates
a complete Markdown report.

Pipeline:
  1. Load Train + Val + Test data from signal_mind.duckdb (READ-ONLY).
  2. For each hypothesis (instrument, topic, lag):
       a. Build DataFrame from Train data.
       b. Run M5 (VAR/IRF) and M6 (LightGBM walk-forward).
       c. Confirmed iff BOTH confirm (AND rule).
  3. For confirmed Train signals: repeat on Val → holdout check.
  4. For confirmed Train+Val: repeat on Test → final signal.
  5. Write CSV + Markdown report.

Run:
    python -m src.pipeline_v2.train_scanner

Output dir: analytics/phase_b/
"""
from __future__ import annotations

import importlib
import io
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

import warnings
warnings.filterwarnings("ignore")  # suppress sklearn/statsmodels verbosity

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                  line_buffering=True)

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "db" / "signal_mind.duckdb"
OUT_DIR = ROOT / "analytics" / "phase_b"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TOPICS = ["oil", "rate", "ruble", "sanctions", "inflation", "banking", "gold"]
LAGS = [1, 7, 14, 30, 60, 90]

# Instruments with >= 350 trading days in Train (enough for m6 N_MIN=200
# after joining with daily news)
INSTRUMENTS = [
    "USD_RUB", "EUR_RUB", "BRENT", "SP500",
    "MSCI_WORLD", "DXY", "MSCI_INDIA",
    "CHINA_H_SHARES", "FTSE_CHINA_50",
    "GOLD", "SILVER",
]

M5_MODULE = "analytics.testbed.methods.m5_var"
M6_MODULE = "analytics.testbed.methods.m6_lgbm"

# ---------------------------------------------------------------------------
# Data loader
# ---------------------------------------------------------------------------

def load_split(split: str, instrument: str) -> pd.DataFrame | None:
    """Load (news + market + key_rate) for one split and instrument.

    split: 'train' | 'val' | 'test'
    Returns DataFrame with columns: date, market_return, key_rate_pct,
    + all 7 news topics.
    Returns None if not enough rows.
    """
    try:
        import duckdb
    except ImportError:
        raise RuntimeError("duckdb not installed")

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        news_tbl = f"v_{split}_news"
        market_tbl = f"v_{split}_market_data"

        news_df = con.execute(f"SELECT * FROM {news_tbl}").fetchdf()
        news_df = news_df.rename(columns={"news_date": "date"})
        news_df["date"] = pd.to_datetime(news_df["date"])

        mkt_df = con.execute(
            f"SELECT trade_date AS date, close FROM {market_tbl} "
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

        mkt_df = pd.merge_asof(
            mkt_df.sort_values("date"),
            kr_df.sort_values("date"),
            on="date", direction="backward",
        )
        df = pd.merge(mkt_df, news_df, on="date", how="inner")
        df = df.sort_values("date").reset_index(drop=True)
    finally:
        con.close()

    if len(df) < 50:
        return None
    return df


# ---------------------------------------------------------------------------
# Ensemble evaluation
# ---------------------------------------------------------------------------

def run_ensemble(df: pd.DataFrame, topic: str, lag: int,
                 m5, m6) -> dict:
    """Run M5+M6 on one hypothesis. Returns verdict dict."""
    from analytics.testbed.methods.base import Hypothesis
    hyp = Hypothesis(
        news_field=topic,
        target_field="market_return",
        lag_days=lag,
    )
    try:
        v5 = m5.evaluate(df, hyp)
    except Exception as e:
        return {"m5_confirmed": False, "m6_confirmed": False,
                "confirmed": False, "error": f"m5: {e}"}
    try:
        v6 = m6.evaluate(df, hyp)
    except Exception as e:
        return {"m5_confirmed": False, "m6_confirmed": False,
                "confirmed": False, "error": f"m6: {e}"}

    confirmed = v5.confirmed and v6.confirmed
    return {
        "m5_confirmed": v5.confirmed,
        "m5_score": round(float(v5.score), 6),
        "m5_pvalue": round(float(v5.p_value or 0), 6),
        "m5_n": v5.n,
        "m5_reason": v5.extra.get("gate_reason", ""),
        "m6_confirmed": v6.confirmed,
        "m6_score": round(float(v6.score), 6),
        "m6_pvalue": round(float(v6.p_value or 0), 6),
        "m6_n": v6.n,
        "m6_reason": v6.extra.get("gate_reason", ""),
        "confirmed": confirmed,
        "error": "",
    }


# ---------------------------------------------------------------------------
# Main scan
# ---------------------------------------------------------------------------

def main() -> None:
    start_time = time.time()
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_path = OUT_DIR / f"scan_{ts}.log"
    log_fh = open(log_path, "w", encoding="utf-8", buffering=1)

    def log(msg: str) -> None:
        t = datetime.now(timezone.utc).strftime("%H:%M:%S")
        line = f"[{t}] {msg}"
        print(line)
        log_fh.write(line + "\n")

    log(f"Phase B Train Scanner started")
    log(f"DB: {DB_PATH}")
    log(f"Instruments: {INSTRUMENTS}")
    log(f"Topics: {TOPICS}")
    log(f"Lags: {LAGS}")
    log(f"Hypotheses per instrument: {len(TOPICS) * len(LAGS)}")
    log(f"Total Train hypotheses: {len(INSTRUMENTS) * len(TOPICS) * len(LAGS)}")
    log("-" * 60)

    # Load ensemble methods once
    m5 = importlib.import_module(M5_MODULE).build()
    m6 = importlib.import_module(M6_MODULE).build()
    log(f"Ensemble: M5={m5.name} AND M6={m6.name}")

    # Pre-load split dataframes (one per instrument)
    log("Pre-loading Train data ...")
    train_dfs: dict[str, pd.DataFrame | None] = {}
    for inst in INSTRUMENTS:
        df = load_split("train", inst)
        train_dfs[inst] = df
        n = len(df) if df is not None else 0
        log(f"  {inst}: {n} rows")

    log("Pre-loading Val data ...")
    val_dfs: dict[str, pd.DataFrame | None] = {}
    for inst in INSTRUMENTS:
        df = load_split("val", inst)
        val_dfs[inst] = df
        n = len(df) if df is not None else 0
        log(f"  {inst}: {n} rows")

    log("Pre-loading Test data ...")
    test_dfs: dict[str, pd.DataFrame | None] = {}
    for inst in INSTRUMENTS:
        df = load_split("test", inst)
        test_dfs[inst] = df
        n = len(df) if df is not None else 0
        log(f"  {inst}: {n} rows")

    log("-" * 60)

    # -----------------------------------------------------------------------
    # Train scan
    # -----------------------------------------------------------------------
    train_rows: list[dict] = []
    total = len(INSTRUMENTS) * len(TOPICS) * len(LAGS)
    done = 0
    confirmed_train = 0

    log(f"Starting Train scan ({total} hypotheses) ...")

    for inst in INSTRUMENTS:
        df_tr = train_dfs[inst]
        if df_tr is None:
            log(f"  SKIP {inst}: no Train data")
            continue
        for topic in TOPICS:
            if topic not in df_tr.columns:
                continue
            for lag in LAGS:
                t0 = time.time()
                result = run_ensemble(df_tr, topic, lag, m5, m6)
                dt = time.time() - t0
                done += 1
                row = {
                    "split": "train",
                    "instrument": inst,
                    "topic": topic,
                    "lag": lag,
                    **result,
                    "elapsed_s": round(dt, 2),
                }
                train_rows.append(row)
                if result["confirmed"]:
                    confirmed_train += 1
                    log(f"  CONFIRMED  {inst:14s} {topic:12s} lag={lag:3d}  "
                        f"m5_p={result.get('m5_pvalue',0):.4f}  "
                        f"m6_ic={result.get('m6_score',0):.4f}  "
                        f"({dt:.1f}s)")
                elif done % 50 == 0:
                    log(f"  progress {done}/{total} "
                        f"({100*done/total:.0f}%)  "
                        f"confirmed so far: {confirmed_train}")

    log("-" * 60)
    log(f"Train scan complete: {confirmed_train}/{total} confirmed")

    # -----------------------------------------------------------------------
    # Val validation for confirmed Train signals
    # -----------------------------------------------------------------------
    val_rows: list[dict] = []
    confirmed_val = 0
    confirmed_train_rows = [r for r in train_rows if r["confirmed"]]

    if confirmed_train_rows:
        log(f"Validating {len(confirmed_train_rows)} confirmed Train signals on Val ...")
        for tr_row in confirmed_train_rows:
            inst = tr_row["instrument"]
            topic = tr_row["topic"]
            lag = tr_row["lag"]
            df_v = val_dfs[inst]
            if df_v is None:
                log(f"  SKIP Val {inst}: no data")
                continue
            result = run_ensemble(df_v, topic, lag, m5, m6)
            if result["confirmed"]:
                confirmed_val += 1
            row = {
                "split": "val",
                "instrument": inst,
                "topic": topic,
                "lag": lag,
                **result,
                "elapsed_s": 0,
            }
            val_rows.append(row)
            status = "HOLD" if result["confirmed"] else "FAIL"
            log(f"  Val {status}  {inst:14s} {topic:12s} lag={lag:3d}  "
                f"m5={result.get('m5_confirmed',False)}  "
                f"m6={result.get('m6_confirmed',False)}")
        log(f"Val holdout: {confirmed_val}/{len(confirmed_train_rows)} hold")
    else:
        log("No confirmed Train signals — skipping Val check")

    # -----------------------------------------------------------------------
    # Test check for Train+Val double-confirmed
    # -----------------------------------------------------------------------
    test_rows: list[dict] = []
    confirmed_test = 0
    confirmed_val_rows = [r for r in val_rows if r["confirmed"]]

    if confirmed_val_rows:
        log(f"Checking {len(confirmed_val_rows)} Train+Val signals on Test ...")
        for v_row in confirmed_val_rows:
            inst = v_row["instrument"]
            topic = v_row["topic"]
            lag = v_row["lag"]
            df_t = test_dfs[inst]
            if df_t is None:
                log(f"  SKIP Test {inst}: no data")
                continue
            result = run_ensemble(df_t, topic, lag, m5, m6)
            if result["confirmed"]:
                confirmed_test += 1
            row = {
                "split": "test",
                "instrument": inst,
                "topic": topic,
                "lag": lag,
                **result,
                "elapsed_s": 0,
            }
            test_rows.append(row)
            status = "LIVE SIGNAL" if result["confirmed"] else "not confirmed"
            log(f"  Test {status}  {inst:14s} {topic:12s} lag={lag:3d}")
    else:
        log("No double-confirmed signals — skipping Test check")

    # -----------------------------------------------------------------------
    # Save results
    # -----------------------------------------------------------------------
    all_rows = train_rows + val_rows + test_rows
    all_df = pd.DataFrame(all_rows)
    csv_path = OUT_DIR / f"scan_{ts}_results.csv"
    all_df.to_csv(csv_path, index=False)
    log(f"Results CSV: {csv_path}")

    # -----------------------------------------------------------------------
    # Markdown report
    # -----------------------------------------------------------------------
    elapsed = time.time() - start_time
    md_path = OUT_DIR / f"scan_{ts}_report.md"
    lines: list[str] = [
        "# Phase B — Train Scanner Report",
        "",
        f"Run timestamp: {datetime.now(timezone.utc).isoformat()}",
        f"Elapsed: {elapsed:.0f}s ({elapsed/60:.1f} min)",
        "",
        "## Summary",
        "",
        f"| Split | Hypotheses | Confirmed | Rate |",
        f"|---|---|---|---|",
        f"| Train | {total} | {confirmed_train} | {confirmed_train/total:.1%} |",
        f"| Val   | {len(val_rows)} | {confirmed_val} | {confirmed_val/max(1,len(val_rows)):.1%} |",
        f"| Test  | {len(test_rows)} | {confirmed_test} | {confirmed_test/max(1,len(test_rows)):.1%} |",
        "",
    ]

    # Confirmed Train signals
    if confirmed_train_rows:
        lines += [
            "## Confirmed Train Signals",
            "",
            "| Instrument | Topic | Lag | M5 p-val | M6 IC | Val | Test |",
            "|---|---|---|---|---|---|---|",
        ]
        for tr in confirmed_train_rows:
            inst, topic, lag = tr["instrument"], tr["topic"], tr["lag"]
            val_row = next((r for r in val_rows
                            if r["instrument"] == inst
                            and r["topic"] == topic
                            and r["lag"] == lag), None)
            test_row = next((r for r in test_rows
                             if r["instrument"] == inst
                             and r["topic"] == topic
                             and r["lag"] == lag), None)
            val_ok = ("HOLD" if val_row and val_row["confirmed"]
                      else ("FAIL" if val_row else "—"))
            test_ok = ("LIVE" if test_row and test_row["confirmed"]
                       else ("fail" if test_row else "—"))
            lines.append(
                f"| {inst} | {topic} | {lag} "
                f"| {tr.get('m5_pvalue',0):.4f} "
                f"| {tr.get('m6_score',0):.4f} "
                f"| {val_ok} | {test_ok} |"
            )
        lines.append("")
    else:
        lines += ["## No confirmed Train signals", ""]

    # Live signals (confirmed on all three splits)
    live = [r for r in test_rows if r.get("confirmed")]
    if live:
        lines += [
            "## LIVE SIGNALS (Train + Val + Test confirmed)",
            "",
            "These signals passed ALL three time periods.",
            "",
        ]
        for r in live:
            lines.append(
                f"**{r['instrument']} — {r['topic']} lag={r['lag']}**  "
                f"m5_score={r.get('m5_score',0):.4f}  "
                f"m6_ic={r.get('m6_score',0):.4f}"
            )
        lines.append("")
    else:
        lines += ["## No live signals passed all three splits", ""]

    lines += [
        "---",
        f"Ensemble: M5_var_orth_irf AND M6_lgbm_walkforward",
        f"Config: `analytics/testbed/ensemble/ensemble_config.yaml`",
        f"Full results: `{csv_path.name}`",
        f"Log: `{log_path.name}`",
    ]

    md_path.write_text("\n".join(lines), encoding="utf-8")
    log(f"Report: {md_path}")
    log("=" * 60)
    log(f"DONE. {confirmed_train} Train | {confirmed_val} Val | "
        f"{confirmed_test} Test confirmed.")
    log("=" * 60)
    log_fh.close()


if __name__ == "__main__":
    main()
