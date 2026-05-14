"""Build S2 — spurious trend testbed dataset.

Two series with a SHARED linear trend but no causal link:
  - news_mentions: Poisson with mean rising linearly with time (10 → 30)
  - market_close: geometric walk with slight negative drift (200 → ~120)

Both are trending; on RAW LEVELS, Pearson r will look strong and negative.
On RETURNS (or detrended series), the link vanishes — that is the truth.

This is the canonical trap for the M001 family of mistakes (CORR on levels).
A correct method must NOT confirm a signal here. Expected FPR ≈ 5% on returns,
but ≈ 80%+ on levels (which is exactly what the project's history showed).

100 seeds × 1000 trading days.

Phase A.0a of v2 testbed-first methodology.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

N_DAYS = 1000
N_SEEDS = 100
NEWS_BASE = 10.0
NEWS_TREND_PER_DAY = 0.020       # Poisson mean grows by 0.02/day → +20 over horizon
MARKET_DRIFT_PER_DAY = -0.0006   # log-return drift, ≈ -45% cumulative over 1000 days
MARKET_SIGMA = 0.01
START_DATE = "2022-01-01"

OUT_DIR = Path(__file__).resolve().parents[1] / "datasets" / "S2"


def build_one_seed(seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start=START_DATE, periods=N_DAYS)
    t = np.arange(N_DAYS)

    news_mean = NEWS_BASE + NEWS_TREND_PER_DAY * t
    news_mentions = rng.poisson(lam=news_mean).astype(np.int32)

    log_returns = rng.normal(loc=MARKET_DRIFT_PER_DAY, scale=MARKET_SIGMA, size=N_DAYS)
    market_close = 200.0 * np.exp(np.cumsum(log_returns))

    return pd.DataFrame(
        {
            "trade_date": dates,
            "news_mentions": news_mentions,
            "market_close": market_close.astype(np.float64),
            "market_return": log_returns.astype(np.float64),
        }
    )


def write_ground_truth() -> None:
    payload = {
        "dataset": "S2",
        "name": "Spurious trend (CORR-on-levels trap)",
        "description": (
            "Two independent stochastic processes that both happen to trend — "
            "news_mentions has Poisson mean linearly increasing, market_close "
            "drifts down. There is NO causal link, only a shared time index. "
            "A method that correlates on levels will see a strong false signal; "
            "a method that correlates on returns will see none."
        ),
        "n_days": N_DAYS,
        "n_seeds": N_SEEDS,
        "data_generating_process": {
            "news_mentions": (
                f"Poisson(lambda = {NEWS_BASE} + {NEWS_TREND_PER_DAY} * t)"
            ),
            "market_close": (
                f"200 * exp(cumsum(N({MARKET_DRIFT_PER_DAY}, {MARKET_SIGMA}^2)))"
            ),
            "joint_distribution": "independent (only share calendar t)",
        },
        "true_signals": [],
        "expected_method_behavior": {
            "should_detect_any_signal": False,
            "corr_on_levels_expected_fpr": ">= 0.80",
            "corr_on_returns_expected_fpr": "≈ 0.05",
            "comment": (
                "S2 is the headline diagnostic for mistake M001 in the catalogue. "
                "It separates methods that handle non-stationarity from those "
                "that don't. This is also our HOLD-OUT dataset (with S6)."
            ),
        },
        "use_in_phase_A": "HOLD-OUT for ensemble final evaluation",
        "use_for_loss_function": (
            "alpha * FPR_S2 is the strongest signal of M001-compatibility"
        ),
        "tied_to_mistakes": ["M001"],
    }
    out = OUT_DIR / "ground_truth.json"
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write_ground_truth()
    for seed in range(N_SEEDS):
        df = build_one_seed(seed)
        df.to_parquet(OUT_DIR / f"seed_{seed:03d}.parquet",
                      engine="pyarrow", compression="zstd", index=False)
    print(f"[S2] wrote {N_SEEDS} seeds x {N_DAYS} days to {OUT_DIR}")


if __name__ == "__main__":
    main()
