"""Build S5 — multi-signal testbed dataset.

Three news topics, each its own column:
  - topic_A → market_return at lag  7, r=-0.30 (strong, like rate-news)
  - topic_B → market_return at lag 30, r=+0.20 (moderate, like inflation-news)
  - topic_C → no link (pure noise topic)

A correct method must rank A and B as confirmed, C as rejected.
Tests multi-hypothesis discipline: no over-confirming, no missing real signals.

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
NEWS_LAMBDA = 20.0
MARKET_SIGMA = 0.01
START_DATE = "2022-01-01"

# (topic_name, lag, r) — r=0 means no signal
TOPICS = [
    ("topic_A", 7, -0.30),
    ("topic_B", 30, +0.20),
    ("topic_C", 14, 0.0),
]

OUT_DIR = Path(__file__).resolve().parents[1] / "datasets" / "S5"


def _zscore(x: np.ndarray) -> np.ndarray:
    return (x - x.mean()) / (x.std() + 1e-12)


def build_one_seed(seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start=START_DATE, periods=N_DAYS)

    cols: dict[str, np.ndarray] = {"trade_date": dates}

    # Build all news columns first
    news_z_by_topic: dict[str, np.ndarray] = {}
    for name, _, _ in TOPICS:
        counts = rng.poisson(lam=NEWS_LAMBDA, size=N_DAYS).astype(np.int32)
        cols[name] = counts
        news_z_by_topic[name] = _zscore(counts.astype(np.float64))

    # Now construct market_return as a linear combination of lagged news z-scores,
    # weighted so that pairwise (news_topic, market_return) corr ≈ r.
    # Because topics are independent Poisson, their z-scores are approximately
    # orthogonal, so partial r ≈ marginal r.
    eps = _zscore(rng.normal(0.0, 1.0, size=N_DAYS))

    # Total explained variance from signals
    explained_var = sum(r**2 for _, _, r in TOPICS)
    if explained_var >= 1.0:
        raise ValueError("Sum of r^2 must be < 1 for valid mixture")

    combined = np.zeros(N_DAYS)
    for name, lag, r in TOPICS:
        if r == 0.0:
            continue
        shifted = np.zeros(N_DAYS)
        shifted[lag:] = news_z_by_topic[name][: N_DAYS - lag]
        combined += r * shifted
    combined += np.sqrt(1.0 - explained_var) * eps

    log_returns = combined * MARKET_SIGMA
    market_close = 100.0 * np.exp(np.cumsum(log_returns))

    cols["market_close"] = market_close.astype(np.float64)
    cols["market_return"] = log_returns.astype(np.float64)
    return pd.DataFrame(cols)


def write_ground_truth() -> None:
    true_signals = [
        {
            "news_field": name,
            "target_field": "market_return",
            "lag_days": lag,
            "expected_r": r,
            "direction": "negative" if r < 0 else "positive",
        }
        for name, lag, r in TOPICS if r != 0.0
    ]
    payload = {
        "dataset": "S5",
        "name": "Multi-signal — 2 real + 1 noise",
        "description": (
            "Three news topics in one dataset. topic_A has a real -0.30 "
            "correlation with market_return at lag 7; topic_B has +0.20 at "
            "lag 30; topic_C has no link. A correct method ranks A and B "
            "above C, and confirms only A and B."
        ),
        "n_days": N_DAYS,
        "n_seeds": N_SEEDS,
        "data_generating_process": {
            "topics": [
                {"name": n, "process": f"Poisson({NEWS_LAMBDA})"} for n, _, _ in TOPICS
            ],
            "market_return": (
                "sum_i r_i * z(topic_i[t - lag_i]) + sqrt(1 - sum r_i^2) * z(eps), "
                f"scaled by {MARKET_SIGMA}"
            ),
            "topic_specs": [
                {"name": n, "lag": l, "r": r} for n, l, r in TOPICS
            ],
        },
        "true_signals": true_signals,
        "noise_signals": [
            {
                "news_field": "topic_C",
                "comment": "No causal link — should not be confirmed at any lag",
            }
        ],
        "expected_method_behavior": {
            "should_confirm": ["topic_A", "topic_B"],
            "should_reject": ["topic_C"],
            "comment": (
                "Tests multi-hypothesis discipline. With BH FDR at q<0.1, "
                "an unbiased pipeline rejects topic_C while keeping A and B."
            ),
        },
        "use_in_phase_A": "CALIBRATION (with S1, S3, S4)",
        "use_for_loss_function": (
            "0.5 * (1 - TPR_real) + 0.5 * FPR_noise_topic"
        ),
        "tied_to_mistakes": ["M002", "M004"],
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
    print(f"[S5] wrote {N_SEEDS} seeds x {N_DAYS} days to {OUT_DIR}")


if __name__ == "__main__":
    main()
