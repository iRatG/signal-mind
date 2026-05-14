"""Build S4 — regime-dependent signal testbed dataset.

The signal exists ONLY when key_rate_pct > 15 (high-rate regime).
In low-rate regime the news–market link is zero.

First half of the time series: high_rate regime, signal active (r ≈ -0.5 at lag 7).
Second half: low_rate regime, no signal.

A regime-blind method will see a diluted average signal (~-0.25) and may or
may not confirm. A regime-aware method must confirm in the high regime and
reject in the low regime.

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
LAG = 7
SIGNAL_R_HIGH = -0.50         # r in high_rate regime
SIGNAL_R_LOW = 0.0            # r in low_rate regime
HIGH_RATE_VALUE = 18.0
LOW_RATE_VALUE = 8.0
RATE_NOISE = 0.5
NEWS_LAMBDA = 20.0
MARKET_SIGMA = 0.01
START_DATE = "2022-01-01"

OUT_DIR = Path(__file__).resolve().parents[1] / "datasets" / "S4"


def _zscore(x: np.ndarray) -> np.ndarray:
    return (x - x.mean()) / (x.std() + 1e-12)


def build_one_seed(seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start=START_DATE, periods=N_DAYS)
    half = N_DAYS // 2

    regime = np.array(["high_rate"] * half + ["low_rate"] * (N_DAYS - half))
    key_rate_pct = np.where(regime == "high_rate", HIGH_RATE_VALUE, LOW_RATE_VALUE) \
        + rng.normal(0.0, RATE_NOISE, size=N_DAYS)

    news_mentions = rng.poisson(lam=NEWS_LAMBDA, size=N_DAYS).astype(np.int32)
    news_z = _zscore(news_mentions.astype(np.float64))

    shifted_news = np.zeros(N_DAYS)
    shifted_news[LAG:] = news_z[: N_DAYS - LAG]

    eps = _zscore(rng.normal(0.0, 1.0, size=N_DAYS))
    r_per_day = np.where(regime == "high_rate", SIGNAL_R_HIGH, SIGNAL_R_LOW)

    combined = r_per_day * shifted_news + np.sqrt(1.0 - r_per_day**2) * eps
    log_returns = combined * MARKET_SIGMA
    market_close = 100.0 * np.exp(np.cumsum(log_returns))

    return pd.DataFrame(
        {
            "trade_date": dates,
            "news_mentions": news_mentions,
            "market_close": market_close.astype(np.float64),
            "market_return": log_returns.astype(np.float64),
            "key_rate_pct": key_rate_pct.astype(np.float64),
            "regime": regime,
        }
    )


def write_ground_truth() -> None:
    payload = {
        "dataset": "S4",
        "name": "Regime-dependent signal",
        "description": (
            "Signal active only in high_rate regime (first half of series, "
            f"key_rate≈{HIGH_RATE_VALUE}%); zero in low_rate regime (second "
            f"half, key_rate≈{LOW_RATE_VALUE}%). Lag {LAG}, "
            f"r_high={SIGNAL_R_HIGH} on returns."
        ),
        "n_days": N_DAYS,
        "n_seeds": N_SEEDS,
        "data_generating_process": {
            "regime": "first half high_rate, second half low_rate",
            "key_rate_pct": (
                f"{HIGH_RATE_VALUE} in high_rate else {LOW_RATE_VALUE}, "
                f"plus N(0, {RATE_NOISE}) noise"
            ),
            "news_mentions": f"Poisson({NEWS_LAMBDA})",
            "market_return": (
                f"r * z(news[t-{LAG}]) + sqrt(1-r^2) * z(eps), "
                f"r=r_high in high_rate else 0, scaled by {MARKET_SIGMA}"
            ),
        },
        "true_signals": [
            {
                "news_field": "news_mentions",
                "target_field": "market_return",
                "lag_days": LAG,
                "expected_r_in_regime": SIGNAL_R_HIGH,
                "regime_filter": "key_rate_pct > 15",
                "direction": "negative",
            }
        ],
        "expected_method_behavior": {
            "regime_aware_method": "should detect strong signal in high_rate, reject in low_rate",
            "regime_blind_method": "should see diluted signal (~-0.25), borderline confirm",
            "comment": (
                "Distinguishes methods that handle regime conditioning from "
                "those that don't. Calibration dataset."
            ),
        },
        "use_in_phase_A": "CALIBRATION (with S1, S3, S5)",
        "use_for_loss_function": "beta * (1 - TPR_S4_in_high_regime)",
        "tied_to_mistakes": ["M003"],
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
    print(f"[S4] wrote {N_SEEDS} seeds x {N_DAYS} days to {OUT_DIR}")


if __name__ == "__main__":
    main()
