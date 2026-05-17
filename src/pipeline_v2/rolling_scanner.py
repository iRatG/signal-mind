"""Rolling Window Scanner — 8-hour overnight job.

Slides a 6-month window across the entire merged train+val dataset
(2022-01-01 to 2025-04-30) with a 1-day step. For each window position
runs M5+M6 ensemble on all (instrument, topic, lag) combinations.

This maps out:
  - WHICH signals are stable across time (appear in >30% of windows)
  - WHERE signal strength peaks (exact date ranges)
  - How signal strength evolves over the Russia/Ukraine/sanctions cycle

Config:
  WINDOW_DAYS = 125   (approx 6 months of trading days)
  STEP_DAYS   = 3     (new window every 3 calendar days = ~2 trading days)
  LAGS        = [1, 7, 14, 30, 60]   (drops 90 for speed)
  INSTRUMENTS = top-7 by data quality

Estimated runtime:
  ~(n_windows) x (n_hyps) x (0.3 sec/hyp)
  ~290 windows x 7x7x5=245 hyps x 0.3s = ~21,315s = ~6 hours

Output: analytics/phase_b/rolling_<ts>_results.csv
        analytics/phase_b/rolling_<ts>_report.md  (stability table)
"""
from __future__ import annotations

import importlib
import io
import sys
import time
import warnings
from datetime import datetime, timedelta, timezone
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

# Window must be large enough for m6's N_MIN=200 after lag removal.
# 260 trading days / 0.72 trading-day fraction = ~361 calendar days.
# Step=7 calendar days, 5 instruments x 7 topics x 4 lags = 140 hyps/window.
# ~137 windows x 140 hyps x ~1.5 sec/hyp = ~28700 sec = ~8 hours overnight.
WINDOW_DAYS = 260    # ~1 year of trading days
STEP_DAYS = 7        # step every 7 calendar days (~weekly)
TOPICS = ["oil", "rate", "ruble", "sanctions", "inflation", "banking", "gold"]
# 4 lags covers short-to-medium range; drops 60 for speed
LAGS = [1, 7, 14, 30]

# 5 core instruments (RU-adjacent + global references)
INSTRUMENTS = [
    "USD_RUB", "EUR_RUB", "BRENT", "SP500", "MSCI_WORLD",
]

M5_MODULE = "analytics.testbed.methods.m5_var"
M6_MODULE = "analytics.testbed.methods.m6_lgbm"

# Research threshold (less strict than production for discovery)
M5_P_RESEARCH = 0.05


def load_full_merged(instrument: str) -> pd.DataFrame | None:
    """Load ALL available data (train + val) for one instrument."""
    try:
        import duckdb
    except ImportError:
        raise RuntimeError("duckdb not installed")
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        # Combine train + val news
        news_q = """
            SELECT news_date AS date, oil, rate, ruble, sanctions,
                   inflation, banking, gold FROM v_train_news
            UNION ALL
            SELECT news_date, oil, rate, ruble, sanctions,
                   inflation, banking, gold FROM v_val_news
            ORDER BY date
        """
        news_df = con.execute(news_q).fetchdf()
        news_df["date"] = pd.to_datetime(news_df["date"])
        news_df = news_df.drop_duplicates("date").sort_values("date").reset_index(drop=True)

        mkt_q = f"""
            SELECT trade_date AS date, close
            FROM v_train_market_data WHERE instrument = '{instrument}'
            UNION ALL
            SELECT trade_date, close
            FROM v_val_market_data WHERE instrument = '{instrument}'
            ORDER BY trade_date
        """
        mkt_df = con.execute(mkt_q).fetchdf()
        if len(mkt_df) < 100:
            return None
        mkt_df["date"] = pd.to_datetime(mkt_df["date"])
        mkt_df = mkt_df.drop_duplicates("date").sort_values("date").reset_index(drop=True)
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
    if len(df) < 100:
        return None
    return df


def run_one(df_window: pd.DataFrame, topic: str, lag: int, m5, m6) -> dict:
    from analytics.testbed.methods.base import Hypothesis
    hyp = Hypothesis(news_field=topic, target_field="market_return", lag_days=lag)
    try:
        v5 = m5.evaluate(df_window, hyp)
        v6 = m6.evaluate(df_window, hyp)
    except Exception as e:
        return {"m5_p": 1.0, "m5_score": 0.0, "m6_ic": 0.0,
                "m6_p": 1.0, "m5_n": 0, "m6_n": 0,
                "research": False, "error": str(e)[:100]}
    return {
        "m5_p": round(float(v5.p_value or 1.0), 6),
        "m5_score": round(float(v5.score), 6),
        "m5_n": v5.n,
        "m6_ic": round(float(v6.score), 6),
        "m6_p": round(float(v6.p_value or 1.0), 6),
        "m6_n": v6.n,
        "research": (float(v5.p_value or 1.0) < M5_P_RESEARCH
                     and v6.confirmed),
        "production": v5.confirmed and v6.confirmed,
        "error": "",
    }


def main() -> None:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    start = time.time()

    print(f"Rolling Window Scanner — started {ts}")
    print(f"Window: {WINDOW_DAYS} trading days, step every {STEP_DAYS} calendar days")
    print(f"Instruments: {INSTRUMENTS}")
    print(f"Topics x Lags: {len(TOPICS)} x {len(LAGS)} = {len(TOPICS)*len(LAGS)} per window")

    m5 = importlib.import_module(M5_MODULE).build()
    m6 = importlib.import_module(M6_MODULE).build()

    # Load full merged data per instrument
    full_dfs: dict[str, pd.DataFrame | None] = {}
    for inst in INSTRUMENTS:
        df = load_full_merged(inst)
        full_dfs[inst] = df
        n = len(df) if df is not None else 0
        date_range = (f"{df['date'].min().date()} to {df['date'].max().date()}"
                      if df is not None else "no data")
        print(f"  {inst}: {n} rows  {date_range}")

    # Build window date grid from min to max common date
    all_dates: list[pd.Timestamp] = []
    for df in full_dfs.values():
        if df is not None:
            all_dates.extend(df["date"].tolist())
    all_dates_sorted = sorted(set(all_dates))
    if not all_dates_sorted:
        print("ERROR: no data loaded")
        return

    global_start = all_dates_sorted[0]
    global_end = all_dates_sorted[-1]

    # Generate window start dates (calendar days step)
    window_starts: list[pd.Timestamp] = []
    cur = global_start
    while cur <= global_end - timedelta(days=WINDOW_DAYS):
        window_starts.append(cur)
        cur = cur + timedelta(days=STEP_DAYS)

    n_windows = len(window_starts)
    n_hyps = len(INSTRUMENTS) * len(TOPICS) * len(LAGS)
    print(f"\n{n_windows} windows x {n_hyps} hyps/window = "
          f"{n_windows * n_hyps} total runs")

    # Estimate time
    # Measured: ~1.5-2 sec per hypothesis (VAR + LightGBM on 260-row window)
    samp_per_sec = 0.65
    eta_sec = n_windows * n_hyps / samp_per_sec
    print(f"Estimated runtime: {eta_sec/3600:.1f} hours")
    print("-" * 60)

    csv_path = OUT_DIR / f"rolling_{ts}_results.csv"
    # Stream results to CSV incrementally
    csv_fh = open(csv_path, "w", encoding="utf-8", buffering=1)
    header = ("window_start,instrument,topic,lag,"
              "m5_p,m5_score,m5_n,m6_ic,m6_p,m6_n,"
              "research,production,error\n")
    csv_fh.write(header)

    total_done = 0
    total_research = 0
    total_production = 0

    for w_idx, win_start in enumerate(window_starts):
        # Calendar days needed: ~WINDOW_DAYS trading days / 0.72 (trading day fraction)
        win_end = win_start + timedelta(days=int(WINDOW_DAYS / 0.72))

        for inst in INSTRUMENTS:
            df_full = full_dfs[inst]
            if df_full is None:
                continue
            # Slice window
            mask = (df_full["date"] >= win_start) & (df_full["date"] <= win_end)
            df_w = df_full[mask].reset_index(drop=True)
            if len(df_w) < 100:
                continue
            # Keep only WINDOW_DAYS rows max (truncate to last WINDOW_DAYS)
            if len(df_w) > WINDOW_DAYS:
                df_w = df_w.tail(WINDOW_DAYS).reset_index(drop=True)

            for topic in TOPICS:
                if topic not in df_w.columns:
                    continue
                for lag in LAGS:
                    result = run_one(df_w, topic, lag, m5, m6)
                    csv_fh.write(
                        f"{win_start.date()},{inst},{topic},{lag},"
                        f"{result['m5_p']},{result['m5_score']},"
                        f"{result['m5_n']},{result['m6_ic']},"
                        f"{result['m6_p']},{result['m6_n']},"
                        f"{result['research']},{result['production']},"
                        f"{result.get('error','')}\n"
                    )
                    total_done += 1
                    if result["research"]:
                        total_research += 1
                        print(f"  RESEARCH  {win_start.date()} "
                              f"{inst:12s} {topic:10s} lag={lag:2d} "
                              f"m5_p={result['m5_p']:.4f} "
                              f"m6_ic={result['m6_ic']:.4f}")
                    if result["production"]:
                        total_production += 1
                        print(f"  PRODUCTION {win_start.date()} "
                              f"{inst:12s} {topic:10s} lag={lag:2d}")

        # Progress every 10 windows
        if (w_idx + 1) % 10 == 0:
            elapsed = time.time() - start
            eta = elapsed / (w_idx + 1) * (n_windows - w_idx - 1)
            print(f"  window {w_idx+1}/{n_windows} "
                  f"({100*(w_idx+1)/n_windows:.0f}%)  "
                  f"research={total_research}  prod={total_production}  "
                  f"eta={eta/3600:.1f}h")

    csv_fh.close()

    elapsed = time.time() - start
    print(f"\nDone in {elapsed/3600:.1f}h ({elapsed/60:.0f} min)")
    print(f"Total runs: {total_done}")
    print(f"Research signals: {total_research}")
    print(f"Production signals: {total_production}")
    print(f"CSV: {csv_path}")

    # Generate summary report
    result_df = pd.read_csv(csv_path)
    md_path = OUT_DIR / f"rolling_{ts}_report.md"

    # Stability: for each (instrument, topic, lag) count % windows confirmed
    total_wins = result_df.groupby(["instrument", "topic", "lag"]).size().rename("n_windows")
    res_wins = result_df[result_df["research"]].groupby(["instrument", "topic", "lag"]).size().rename("n_research")
    prod_wins = result_df[result_df["production"]].groupby(["instrument", "topic", "lag"]).size().rename("n_production")
    stability = (total_wins.to_frame()
                 .join(res_wins, how="left")
                 .join(prod_wins, how="left")
                 .fillna(0)
                 .assign(research_rate=lambda d: d.n_research / d.n_windows,
                         prod_rate=lambda d: d.n_production / d.n_windows)
                 .sort_values("research_rate", ascending=False)
                 .reset_index())

    lines: list[str] = [
        "# Rolling Window Scan Report",
        "",
        f"Run: {datetime.now(timezone.utc).isoformat()}",
        f"Elapsed: {elapsed/3600:.1f}h",
        f"Windows: {n_windows}, Hypotheses/window: {n_hyps}",
        "",
        "## Summary",
        "",
        f"| Mode | Total Confirmations | Note |",
        f"|---|---|---|",
        f"| Research (p<0.05 AND IC>0.03) | {int(total_research)} | exploratory |",
        f"| Production (p<0.01 AND IC>0.03) | {int(total_production)} | strict |",
        "",
        "## Signal Stability — Top 30 by Research Confirmation Rate",
        "",
        "*(Stability = fraction of rolling windows where signal was confirmed)*",
        "",
        "| Instrument | Topic | Lag | Research Rate | Prod Rate | n_windows |",
        "|---|---|---|---|---|---|",
    ]
    for _, r in stability.head(30).iterrows():
        lines.append(
            f"| {r['instrument']} | {r['topic']} | {int(r['lag'])} "
            f"| {r['research_rate']:.1%} | {r['prod_rate']:.1%} "
            f"| {int(r['n_windows'])} |"
        )
    lines += ["", f"Full data: `{csv_path.name}`"]
    md_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Report: {md_path}")


if __name__ == "__main__":
    main()
