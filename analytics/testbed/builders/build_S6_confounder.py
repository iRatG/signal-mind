"""Build S6 — confounder testbed dataset.

A hidden third variable (key_rate_pct, AR(1) process) drives BOTH news volume
and market returns. There is NO direct causal link news → market.

A pure correlation method will see a strong (spurious) correlation between
news and market. A method with Granger-causality discipline, or one that
controls for key_rate, should NOT confirm a direct news → market signal.

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
KEY_RATE_MEAN = 12.0
KEY_RATE_INNOVATION_SIGMA = 0.5
KEY_RATE_PERSISTENCE = 0.97          # AR(1) coefficient
NEWS_BASE = 10.0
NEWS_RATE_LOADING = 0.6              # news mean ≈ base + loading * (rate - mean)
MARKET_RATE_LOADING = -0.0015         # log-return loaded by (rate - mean)
MARKET_SIGMA = 0.01
START_DATE = "2022-01-01"

OUT_DIR = Path(__file__).resolve().parents[1] / "datasets" / "S6"


def _ar1(rng: np.random.Generator, n: int, mean: float,
         persistence: float, innovation_sigma: float) -> np.ndarray:
    """Generate an AR(1) process around `mean`."""
    out = np.empty(n)
    out[0] = mean + rng.normal(0.0, innovation_sigma)
    for t in range(1, n):
        out[t] = mean + persistence * (out[t - 1] - mean) \
            + rng.normal(0.0, innovation_sigma)
    return out


def build_one_seed(seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start=START_DATE, periods=N_DAYS)

    key_rate_pct = _ar1(rng, N_DAYS, KEY_RATE_MEAN,
                        KEY_RATE_PERSISTENCE, KEY_RATE_INNOVATION_SIGMA)

    # News count Poisson with rate-driven mean (more news when rates are high)
    news_mean = np.clip(NEWS_BASE + NEWS_RATE_LOADING * (key_rate_pct - KEY_RATE_MEAN),
                        a_min=1.0, a_max=None)
    news_mentions = rng.poisson(lam=news_mean).astype(np.int32)

    # Market returns loaded by rate (negative loading — rate up → market down),
    # plus noise. NO direct news term.
    log_returns = MARKET_RATE_LOADING * (key_rate_pct - KEY_RATE_MEAN) \
        + rng.normal(0.0, MARKET_SIGMA, size=N_DAYS)
    market_close = 100.0 * np.exp(np.cumsum(log_returns))

    return pd.DataFrame(
        {
            "trade_date": dates,
            "news_mentions": news_mentions,
            "market_close": market_close.astype(np.float64),
            "market_return": log_returns.astype(np.float64),
            "key_rate_pct": key_rate_pct.astype(np.float64),
        }
    )


def write_ground_truth() -> None:
    payload = {
        "dataset": "S6",
        "name": "Hidden confounder (key_rate drives both)",
        "description": (
            "key_rate_pct follows a slow AR(1) process. Both news_mentions and "
            "market_return are driven by it: more news when rates are high, "
            "market falls when rates rise. There is NO direct causal "
            "news → market mechanism. A naive correlation method will "
            "wrongly confirm a signal; a Granger-causal method, or one that "
            "conditions on key_rate, should not."
        ),
        "n_days": N_DAYS,
        "n_seeds": N_SEEDS,
        "data_generating_process": {
            "key_rate_pct": (
                f"AR(1) around mean {KEY_RATE_MEAN}, persistence "
                f"{KEY_RATE_PERSISTENCE}, innovation sigma "
                f"{KEY_RATE_INNOVATION_SIGMA}"
            ),
            "news_mentions": (
                f"Poisson(lambda = max(1, {NEWS_BASE} + "
                f"{NEWS_RATE_LOADING} * (rate - mean)))"
            ),
            "market_return": (
                f"{MARKET_RATE_LOADING} * (rate - mean) + N(0, {MARKET_SIGMA}^2)"
            ),
            "direct_news_to_market_link": "NONE — confounded by key_rate",
        },
        "true_signals": [],
        "expected_method_behavior": {
            "naive_correlation": "will see spurious correlation, false confirm",
            "granger_causal": "should not confirm news→market once rate is conditioned",
            "var_with_rate": "impulse response of market to news should be near zero",
            "comment": (
                "This is the second HOLD-OUT dataset. It distinguishes "
                "methods that handle confounding from those that don't."
            ),
        },
        "use_in_phase_A": "HOLD-OUT for ensemble final evaluation (with S2)",
        "use_for_loss_function": "alpha * FPR_S6 — the causal-discipline check",
        "tied_to_mistakes": ["M008"],
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
    print(f"[S6] wrote {N_SEEDS} seeds x {N_DAYS} days to {OUT_DIR}")


if __name__ == "__main__":
    main()
