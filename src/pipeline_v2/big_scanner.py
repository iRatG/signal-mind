"""Big hypothesis scanner — extended instrument universe.

Tests ALL available instruments (market_data + MOEX sectors) against all
news topics and lags using the frozen M5+M6/AND ensemble.

Instruments (16 total):
  market_data (12): SP500, USD_RUB, EUR_RUB, DJ_SOUTH_AFRICA, MSCI_WORLD,
                    DXY, BRENT, MSCI_INDIA, CHINA_H_SHARES, FTSE_CHINA_50,
                    GOLD, SILVER
  MOEX sectors (4): IMOEX, MOEXFN, MOEXOG, MOEX10

Topics x Lags: 7 x 6 = 42 per instrument
Total hypotheses: 16 x 42 = 672

Run:
    python -m src.pipeline_v2.big_scanner

Output: analytics/phase_b/big_scan_{ts}_results.csv + report.md + .log
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

warnings.filterwarnings("ignore")

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

# Standard market_data instruments
MARKET_INSTRUMENTS = [
    "SP500", "USD_RUB", "EUR_RUB", "DJ_SOUTH_AFRICA",
    "MSCI_WORLD", "DXY", "BRENT", "MSCI_INDIA",
    "CHINA_H_SHARES", "FTSE_CHINA_50", "GOLD", "SILVER",
]

# MOEX sector indices (from v_{split}_sectors wide table)
# key = display name, value = column in v_{split}_sectors
MOEX_SECTORS = {
    "IMOEX":  "imoex",
    "MOEXFN": "moexfn_finance",
    "MOEXOG": "moexog_oil_gas",
    "MOEX10": "moex10_bluechip",
}

M5_MODULE = "analytics.testbed.methods.m5_var"
M6_MODULE = "analytics.testbed.methods.m6_lgbm"

# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def _merge_news_keyrate(mkt_df: pd.DataFrame, split: str, con) -> pd.DataFrame | None:
    """Merge market DataFrame with news and key_rate. Returns None if too small."""
    news_df = con.execute(f"SELECT * FROM v_{split}_news").fetchdf()
    news_df = news_df.rename(columns={"news_date": "date"})
    news_df["date"] = pd.to_datetime(news_df["date"])

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
    if len(df) < 50:
        return None
    return df


def load_market_split(split: str, instrument: str) -> pd.DataFrame | None:
    """Load market_data instrument for one split."""
    try:
        import duckdb
    except ImportError:
        raise RuntimeError("duckdb not installed")

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        mkt_df = con.execute(
            f"SELECT trade_date AS date, close "
            f"FROM v_{split}_market_data "
            f"WHERE instrument = '{instrument}' ORDER BY trade_date"
        ).fetchdf()
        if len(mkt_df) < 30:
            return None
        mkt_df["date"] = pd.to_datetime(mkt_df["date"])
        mkt_df = mkt_df.sort_values("date").reset_index(drop=True)
        mkt_df["market_return"] = np.log(mkt_df["close"]).diff()
        return _merge_news_keyrate(mkt_df, split, con)
    finally:
        con.close()


def load_moex_split(split: str, col: str) -> pd.DataFrame | None:
    """Load a MOEX sector column from v_{split}_sectors."""
    try:
        import duckdb
    except ImportError:
        raise RuntimeError("duckdb not installed")

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        mkt_df = con.execute(
            f"SELECT trade_date AS date, {col} AS close "
            f"FROM v_{split}_sectors "
            f"WHERE {col} IS NOT NULL "
            f"ORDER BY trade_date"
        ).fetchdf()
        if len(mkt_df) < 30:
            return None
        mkt_df["date"] = pd.to_datetime(mkt_df["date"])
        mkt_df = mkt_df.sort_values("date").reset_index(drop=True)
        mkt_df["market_return"] = np.log(mkt_df["close"]).diff()
        return _merge_news_keyrate(mkt_df, split, con)
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Ensemble evaluation
# ---------------------------------------------------------------------------

def run_ensemble(df: pd.DataFrame, topic: str, lag: int, m5, m6) -> dict:
    from analytics.testbed.methods.base import Hypothesis
    hyp = Hypothesis(news_field=topic, target_field="market_return", lag_days=lag)
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
        "m5_score":  round(float(v5.score), 6),
        "m5_pvalue": round(float(v5.p_value or 0), 6),
        "m5_n":      v5.n,
        "m5_reason": v5.extra.get("gate_reason", ""),
        "m6_confirmed": v6.confirmed,
        "m6_score":  round(float(v6.score), 6),
        "m6_pvalue": round(float(v6.p_value or 0), 6),
        "m6_n":      v6.n,
        "m6_reason": v6.extra.get("gate_reason", ""),
        "confirmed": confirmed,
        "error": "",
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    start_time = time.time()
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_path = OUT_DIR / f"big_scan_{ts}.log"
    log_fh = open(log_path, "w", encoding="utf-8", buffering=1)

    def log(msg: str) -> None:
        t = datetime.now(timezone.utc).strftime("%H:%M:%S")
        line = f"[{t}] {msg}"
        print(line)
        log_fh.write(line + "\n")

    all_instruments = MARKET_INSTRUMENTS + list(MOEX_SECTORS.keys())
    total = len(all_instruments) * len(TOPICS) * len(LAGS)

    log("Big Scanner started")
    log(f"Market instruments ({len(MARKET_INSTRUMENTS)}): {MARKET_INSTRUMENTS}")
    log(f"MOEX sectors ({len(MOEX_SECTORS)}): {list(MOEX_SECTORS.keys())}")
    log(f"Topics ({len(TOPICS)}): {TOPICS}")
    log(f"Lags ({len(LAGS)}): {LAGS}")
    log(f"Total hypotheses: {len(all_instruments)} x {len(TOPICS)} x {len(LAGS)} = {total}")
    log("-" * 60)

    m5 = importlib.import_module(M5_MODULE).build()
    m6 = importlib.import_module(M6_MODULE).build()
    log(f"Ensemble: {m5.name} AND {m6.name}")

    # Pre-load all splits
    def preload(split: str) -> dict[str, pd.DataFrame | None]:
        log(f"Loading {split} data ...")
        dfs = {}
        for inst in MARKET_INSTRUMENTS:
            df = load_market_split(split, inst)
            dfs[inst] = df
            log(f"  {inst}: {len(df) if df is not None else 0} rows")
        for name, col in MOEX_SECTORS.items():
            df = load_moex_split(split, col)
            dfs[name] = df
            log(f"  {name} ({col}): {len(df) if df is not None else 0} rows")
        return dfs

    train_dfs = preload("train")
    val_dfs   = preload("val")
    test_dfs  = preload("test")
    log("-" * 60)

    # -----------------------------------------------------------------------
    # Train scan
    # -----------------------------------------------------------------------
    train_rows: list[dict] = []
    done = 0
    confirmed_train = 0

    log(f"Starting Train scan ({total} hypotheses) ...")

    for inst in all_instruments:
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
                    "split": "train", "instrument": inst,
                    "topic": topic, "lag": lag,
                    **result, "elapsed_s": round(dt, 2),
                }
                train_rows.append(row)
                if result["confirmed"]:
                    confirmed_train += 1
                    log(f"  CONFIRMED  {inst:18s} {topic:12s} lag={lag:3d}"
                        f"  m5_p={result.get('m5_pvalue',0):.4f}"
                        f"  m6_ic={result.get('m6_score',0):.4f}"
                        f"  ({dt:.1f}s)")
                elif done % 50 == 0:
                    pct = 100 * done / total
                    log(f"  progress {done}/{total} ({pct:.0f}%)"
                        f"  confirmed: {confirmed_train}")

    log("-" * 60)
    log(f"Train complete: {confirmed_train}/{total} confirmed")

    # -----------------------------------------------------------------------
    # Val holdout
    # -----------------------------------------------------------------------
    val_rows: list[dict] = []
    confirmed_val = 0
    confirmed_train_rows = [r for r in train_rows if r["confirmed"]]

    if confirmed_train_rows:
        log(f"Val holdout: {len(confirmed_train_rows)} signals ...")
        for tr in confirmed_train_rows:
            inst, topic, lag = tr["instrument"], tr["topic"], tr["lag"]
            df_v = val_dfs.get(inst)
            if df_v is None:
                log(f"  SKIP Val {inst}: no data")
                continue
            result = run_ensemble(df_v, topic, lag, m5, m6)
            if result["confirmed"]:
                confirmed_val += 1
            row = {"split": "val", "instrument": inst,
                   "topic": topic, "lag": lag, **result, "elapsed_s": 0}
            val_rows.append(row)
            status = "HOLD" if result["confirmed"] else "FAIL"
            log(f"  Val {status}  {inst:18s} {topic:12s} lag={lag:3d}"
                f"  m5={result.get('m5_confirmed')}  m6={result.get('m6_confirmed')}")
        log(f"Val: {confirmed_val}/{len(confirmed_train_rows)} held")
    else:
        log("No confirmed Train signals — skipping Val")

    # -----------------------------------------------------------------------
    # Test check
    # -----------------------------------------------------------------------
    test_rows: list[dict] = []
    confirmed_test = 0
    confirmed_val_rows = [r for r in val_rows if r["confirmed"]]

    if confirmed_val_rows:
        log(f"Test check: {len(confirmed_val_rows)} signals ...")
        for v_row in confirmed_val_rows:
            inst, topic, lag = v_row["instrument"], v_row["topic"], v_row["lag"]
            df_t = test_dfs.get(inst)
            if df_t is None:
                log(f"  SKIP Test {inst}: no data")
                continue
            result = run_ensemble(df_t, topic, lag, m5, m6)
            if result["confirmed"]:
                confirmed_test += 1
            row = {"split": "test", "instrument": inst,
                   "topic": topic, "lag": lag, **result, "elapsed_s": 0}
            test_rows.append(row)
            status = "LIVE SIGNAL" if result["confirmed"] else "not confirmed"
            log(f"  Test {status}  {inst:18s} {topic:12s} lag={lag:3d}")
    else:
        log("No double-confirmed signals — skipping Test")

    # -----------------------------------------------------------------------
    # Save CSV
    # -----------------------------------------------------------------------
    all_rows = train_rows + val_rows + test_rows
    all_df = pd.DataFrame(all_rows)
    csv_path = OUT_DIR / f"big_scan_{ts}_results.csv"
    all_df.to_csv(csv_path, index=False)
    log(f"CSV: {csv_path}")

    # -----------------------------------------------------------------------
    # Markdown report
    # -----------------------------------------------------------------------
    elapsed = time.time() - start_time
    md_path = OUT_DIR / f"big_scan_{ts}_report.md"

    lines: list[str] = [
        "# Big Scanner Report",
        "",
        f"Run: {datetime.now(timezone.utc).isoformat()}",
        f"Elapsed: {elapsed:.0f}s ({elapsed/60:.1f} min)",
        "",
        "## Summary",
        "",
        f"| Split | Hypotheses | Confirmed | Rate |",
        f"|---|---|---|---|",
        f"| Train | {len(train_rows)} | {confirmed_train} | {confirmed_train/max(1,len(train_rows)):.1%} |",
        f"| Val   | {len(val_rows)} | {confirmed_val} | {confirmed_val/max(1,len(val_rows)):.1%} |",
        f"| Test  | {len(test_rows)} | {confirmed_test} | {confirmed_test/max(1,len(test_rows)):.1%} |",
        "",
    ]

    if confirmed_train_rows:
        lines += [
            "## Confirmed Train Signals",
            "",
            "| Instrument | Topic | Lag | M5 p-val | M6 IC | Val | Test |",
            "|---|---|---|---|---|---|---|",
        ]
        for tr in sorted(confirmed_train_rows,
                         key=lambda r: r.get("m5_pvalue", 1)):
            inst, topic, lag = tr["instrument"], tr["topic"], tr["lag"]
            val_r  = next((r for r in val_rows  if r["instrument"]==inst and r["topic"]==topic and r["lag"]==lag), None)
            test_r = next((r for r in test_rows if r["instrument"]==inst and r["topic"]==topic and r["lag"]==lag), None)
            val_ok  = ("HOLD" if val_r  and val_r["confirmed"]  else ("FAIL" if val_r  else "—"))
            test_ok = ("LIVE" if test_r and test_r["confirmed"] else ("fail" if test_r else "—"))
            lines.append(
                f"| {inst} | {topic} | {lag} "
                f"| {tr.get('m5_pvalue',0):.4f} "
                f"| {tr.get('m6_score',0):.4f} "
                f"| {val_ok} | {test_ok} |"
            )
        lines.append("")
    else:
        lines += ["## No confirmed Train signals", ""]

    # Near-miss: m5 confirmed but not m6, or vice versa
    near_miss = [r for r in train_rows
                 if not r["confirmed"] and not r.get("error")
                 and (r.get("m5_confirmed") or r.get("m6_confirmed"))
                 and r.get("m5_pvalue", 1) < 0.05
                 and r.get("m6_score", 0) > 0.05]
    if near_miss:
        near_miss_sorted = sorted(near_miss,
                                  key=lambda r: r.get("m5_pvalue", 1) + (1 - r.get("m6_score", 0)))[:20]
        lines += [
            "## Near-Misses (one method confirmed, other close)",
            "",
            "*(m5_p < 0.05 AND m6_ic > 0.05, but not both strict)*",
            "",
            "| Instrument | Topic | Lag | M5 conf | M5 p-val | M6 conf | M6 IC |",
            "|---|---|---|---|---|---|---|",
        ]
        for r in near_miss_sorted:
            lines.append(
                f"| {r['instrument']} | {r['topic']} | {r['lag']} "
                f"| {r.get('m5_confirmed')} | {r.get('m5_pvalue',0):.4f} "
                f"| {r.get('m6_confirmed')} | {r.get('m6_score',0):.4f} |"
            )
        lines.append("")

    live = [r for r in test_rows if r.get("confirmed")]
    if live:
        lines += [
            "## LIVE SIGNALS (Train + Val + Test)",
            "",
        ]
        for r in live:
            lines.append(
                f"**{r['instrument']} — {r['topic']} lag={r['lag']}**  "
                f"m5_p={r.get('m5_pvalue',0):.4f}  m6_ic={r.get('m6_score',0):.4f}"
            )
        lines.append("")
    else:
        lines += ["## No live signals passed all three splits", ""]

    lines += [
        "---",
        f"Instruments: {all_instruments}",
        f"Ensemble: M5_var_orth_irf AND M6_lgbm_walkforward",
        f"Full results: `{csv_path.name}`",
        f"Log: `{log_path.name}`",
    ]

    md_path.write_text("\n".join(lines), encoding="utf-8")
    log(f"Report: {md_path}")
    log("=" * 60)
    log(f"DONE. Train={confirmed_train} | Val={confirmed_val} | Test={confirmed_test}")
    log("=" * 60)
    log_fh.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--feature-type",
        choices=["keyword", "embedding"],
        default="keyword",
        help="Feature columns to use: keyword (default) or embedding (*_emb)",
    )
    args, _ = parser.parse_known_args()
    if args.feature_type == "embedding":
        TOPICS[:] = [f"{t}_emb" for t in TOPICS]
    main()
