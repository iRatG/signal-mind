"""Build S3 — known linear signal testbed dataset.

Injects a CONTROLLED relationship:
  market_return[t+LAG] = TRUE_R * standardised(news_mentions[t]) * SIGMA
                       + sqrt(1 - TRUE_R^2) * standardised noise * SIGMA

With LAG=14, TRUE_R=-0.3 — Pearson r should be ≈ -0.3 at lag 14
(asymptotically; per-seed varies due to finite-sample sampling error).

Truth: a method that handles lags correctly MUST detect this at lag 14 and
NOT at lags 1, 7, 30, 60, 90. Direction must be negative.

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
LAG = 14
TRUE_R = -0.30
NEWS_LAMBDA = 20.0
MARKET_SIGMA = 0.01
START_DATE = "2022-01-01"

OUT_DIR = Path(__file__).resolve().parents[1] / "datasets" / "S3"


def _zscore(x: np.ndarray) -> np.ndarray:
    return (x - x.mean()) / (x.std() + 1e-12)


def build_one_seed(seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start=START_DATE, periods=N_DAYS)

    news_mentions = rng.poisson(lam=NEWS_LAMBDA, size=N_DAYS).astype(np.int32)
    news_z = _zscore(news_mentions.astype(np.float64))

    # Shift news forward by LAG so that market_return[t] depends on news[t-LAG].
    shifted_news = np.zeros(N_DAYS)
    shifted_news[LAG:] = news_z[: N_DAYS - LAG]

    eps = _zscore(rng.normal(loc=0.0, scale=1.0, size=N_DAYS))

    # Linear combination yielding Pearson correlation ≈ TRUE_R (asymptotically).
    combined = TRUE_R * shifted_news + np.sqrt(1.0 - TRUE_R**2) * eps
    log_returns = combined * MARKET_SIGMA
    market_close = 100.0 * np.exp(np.cumsum(log_returns))

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
        "dataset": "S3",
        "name": "Known signal at lag 14, r=-0.3",
        "description": (
            "A controlled signal is injected so that market_return[t] is "
            "linearly dependent on standardised news_mentions[t-14] with "
            "Pearson correlation TRUE_R = -0.3 (asymptotic). The dependency "
            "is on RETURNS, not levels."
        ),
        "n_days": N_DAYS,
        "n_seeds": N_SEEDS,
        "data_generating_process": {
            "news_mentions": f"Poisson(lambda={NEWS_LAMBDA})",
            "market_return": (
                f"TRUE_R * z(news[t-{LAG}]) + sqrt(1-TRUE_R^2) * z(eps), "
                f"scaled by sigma={MARKET_SIGMA}"
            ),
            "TRUE_R": TRUE_R,
            "LAG": LAG,
        },
        "true_signals": [
            {
                "news_field": "news_mentions",
                "target_field": "market_return",
                "lag_days": LAG,
                "expected_r": TRUE_R,
                "direction": "negative",
            }
        ],
        "expected_method_behavior": {
            "should_detect_at_lags": [LAG],
            "should_not_detect_at_lags": [1, 7, 30, 60, 90],
            "expected_tpr_at_lag14": ">= 0.80",
            "comment": (
                "If a method's TPR at lag=14 is < 60%, it lacks statistical "
                "power for r=0.3 at n=1000. If it ALSO confirms at other "
                "lags, it has poor lag specificity."
            ),
        },
        "use_in_phase_A": "CALIBRATION (with S1, S4, S5)",
        "use_for_loss_function": "beta * (1 - TPR_S3_at_correct_lag)",
        "tied_to_mistakes": [],
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
    print(f"[S3] wrote {N_SEEDS} seeds x {N_DAYS} days to {OUT_DIR}")


if __name__ == "__main__":
    main()
