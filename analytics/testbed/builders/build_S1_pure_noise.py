"""Build S1 — pure noise testbed dataset.

Two independent series:
  - news_mentions: daily Poisson counts (λ=20), no temporal structure
  - market_close:  geometric random walk (σ=0.01 log-returns), no link to news

Ground truth: NO signal exists. Any method should yield ≈5% confirmed rate
at p<0.05 — that's the definition of correct false-positive rate.

100 seeds × 1000 trading days each → parquet files in datasets/S1/.

Phase A.0a of v2 testbed-first methodology. See:
  - analytics/testbed/README.md
  - memory/project_v2_pivot.md
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

# Configuration (frozen) ------------------------------------------------------
N_DAYS = 1000              # ≈ 4 calendar years of business days
N_SEEDS = 100              # for stable FPR/TPR estimation
NEWS_LAMBDA = 20.0         # mean daily news mentions (Poisson)
MARKET_SIGMA = 0.01        # daily log-return std (typical equity index)
START_DATE = "2022-01-01"  # arbitrary anchor, only used for date join keys

OUT_DIR = Path(__file__).resolve().parents[1] / "datasets" / "S1"


def build_one_seed(seed: int) -> pd.DataFrame:
    """Generate one independent realisation of (news, market) under H0."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start=START_DATE, periods=N_DAYS)

    news_mentions = rng.poisson(lam=NEWS_LAMBDA, size=N_DAYS).astype(np.int32)
    log_returns = rng.normal(loc=0.0, scale=MARKET_SIGMA, size=N_DAYS)
    # Start price 100, geometric walk
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
    """Emit ground_truth.json describing what should happen."""
    payload = {
        "dataset": "S1",
        "name": "Pure noise",
        "description": (
            "Two independent series with no causal link. news_mentions is a "
            "Poisson process (lambda=20), market is a geometric random walk "
            "with sigma=0.01 log-returns. There is NO signal."
        ),
        "n_days": N_DAYS,
        "n_seeds": N_SEEDS,
        "data_generating_process": {
            "news_mentions": f"Poisson(lambda={NEWS_LAMBDA})",
            "market_close": "100 * exp(cumsum(N(0, sigma))) with sigma="
            f"{MARKET_SIGMA}",
            "joint_distribution": "independent",
        },
        "true_signals": [],
        "expected_method_behavior": {
            "should_detect_any_signal": False,
            "expected_fpr_at_p05": 0.05,
            "acceptable_fpr_range_at_p05": [0.02, 0.10],
            "comment": (
                "A method that flags >10% of S1 seeds as containing a signal "
                "is broken — it generates false positives on pure noise."
            ),
        },
        "use_in_phase_A": "FPR baseline; both calibration and hold-out methods",
        "use_for_loss_function": "alpha * FPR_S1 component",
    }
    out = OUT_DIR / "ground_truth.json"
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write_ground_truth()

    for seed in range(N_SEEDS):
        df = build_one_seed(seed)
        out = OUT_DIR / f"seed_{seed:03d}.parquet"
        df.to_parquet(out, engine="pyarrow", compression="zstd", index=False)

    # Sanity printout — no side effects on real DBs.
    print(f"[S1] wrote {N_SEEDS} seeds x {N_DAYS} days to {OUT_DIR}")
    print(f"[S1] ground_truth.json: {OUT_DIR / 'ground_truth.json'}")


if __name__ == "__main__":
    main()
