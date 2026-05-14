"""Verify all 6 testbed datasets — sanity-check the data-generating processes.

Reads ONE seed (seed_000) from each S1..S6 and confirms that empirical
statistics match the design. If any check fails, the builder is buggy and
must be fixed before running methods on testbed.

This script does NOT touch real project data. Read-only.
"""
from __future__ import annotations

import io
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Force UTF-8 on Windows consoles (cp1251 default rejects many symbols).
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)

ROOT = Path(__file__).resolve().parents[1] / "datasets"
TOL = 0.10  # acceptable empirical r deviation from designed r (one-seed slack)


def _corr(a: np.ndarray, b: np.ndarray) -> float:
    return float(np.corrcoef(a, b)[0, 1])


def _lag(arr: np.ndarray, k: int) -> np.ndarray:
    """Return arr shifted forward by k positions (NaN-padded at the start)."""
    out = np.empty_like(arr, dtype=np.float64)
    out[:] = np.nan
    if k < len(arr):
        out[k:] = arr[: len(arr) - k]
    return out


def _corr_with_lag(a: np.ndarray, b: np.ndarray, lag: int) -> float:
    """corr(a_lagged_by_lag, b). NaN-aware."""
    a_l = _lag(a.astype(np.float64), lag)
    mask = ~np.isnan(a_l) & ~np.isnan(b)
    return _corr(a_l[mask], b[mask])


def check_S1() -> dict:
    df = pd.read_parquet(ROOT / "S1" / "seed_000.parquet")
    r_levels = _corr(df["news_mentions"].values, df["market_close"].values)
    r_returns = _corr(df["news_mentions"].values, df["market_return"].values)
    return {
        "name": "S1 pure noise",
        "r_news_market_levels": round(r_levels, 3),
        "r_news_market_returns": round(r_returns, 3),
        "expectation": "both near 0",
        "pass": abs(r_returns) < TOL,
    }


def check_S2() -> dict:
    df = pd.read_parquet(ROOT / "S2" / "seed_000.parquet")
    r_levels = _corr(df["news_mentions"].values, df["market_close"].values)
    r_returns = _corr(df["news_mentions"].values, df["market_return"].values)
    return {
        "name": "S2 spurious trend (M001 trap)",
        "r_news_market_levels": round(r_levels, 3),
        "r_news_market_returns": round(r_returns, 3),
        "expectation": "|r_levels| LARGE, |r_returns| near 0",
        "pass": abs(r_levels) > 0.5 and abs(r_returns) < TOL,
    }


def check_S3() -> dict:
    df = pd.read_parquet(ROOT / "S3" / "seed_000.parquet")
    news = df["news_mentions"].values
    ret = df["market_return"].values
    r_at_correct = _corr_with_lag(news, ret, lag=14)
    r_at_wrong = _corr_with_lag(news, ret, lag=7)
    return {
        "name": "S3 known signal lag=14 r=-0.3",
        "r_lag14": round(r_at_correct, 3),
        "r_lag7": round(r_at_wrong, 3),
        "expectation": "r_lag14 ~= -0.3, r_lag7 ~= 0",
        "pass": (-0.45 < r_at_correct < -0.15) and abs(r_at_wrong) < TOL,
    }


def check_S4() -> dict:
    df = pd.read_parquet(ROOT / "S4" / "seed_000.parquet")
    half = len(df) // 2
    news = df["news_mentions"].values
    ret = df["market_return"].values
    r_high = _corr_with_lag(news[:half], ret[:half], lag=7)
    r_low = _corr_with_lag(news[half:], ret[half:], lag=7)
    return {
        "name": "S4 regime-dependent",
        "r_high_regime_lag7": round(r_high, 3),
        "r_low_regime_lag7": round(r_low, 3),
        "expectation": "r_high ~= -0.5, r_low ~= 0",
        "pass": (-0.65 < r_high < -0.30) and abs(r_low) < 0.15,
    }


def check_S5() -> dict:
    df = pd.read_parquet(ROOT / "S5" / "seed_000.parquet")
    ret = df["market_return"].values
    r_A = _corr_with_lag(df["topic_A"].values, ret, lag=7)
    r_B = _corr_with_lag(df["topic_B"].values, ret, lag=30)
    r_C = _corr_with_lag(df["topic_C"].values, ret, lag=14)
    return {
        "name": "S5 multi-signal (A real, B real, C noise)",
        "r_A_lag7": round(r_A, 3),
        "r_B_lag30": round(r_B, 3),
        "r_C_lag14": round(r_C, 3),
        "expectation": "r_A ~= -0.3, r_B ~= +0.2, r_C ~= 0",
        "pass": (-0.45 < r_A < -0.15) and (0.05 < r_B < 0.35) and abs(r_C) < TOL,
    }


def check_S6() -> dict:
    df = pd.read_parquet(ROOT / "S6" / "seed_000.parquet")
    news = df["news_mentions"].values
    ret = df["market_return"].values
    rate = df["key_rate_pct"].values

    r_news_market = _corr(news, ret)
    r_news_rate = _corr(news, rate)
    r_market_rate = _corr(ret, rate)
    return {
        "name": "S6 confounder",
        "r_news_market": round(r_news_market, 3),
        "r_news_rate": round(r_news_rate, 3),
        "r_market_rate": round(r_market_rate, 3),
        "expectation": "all three nonzero; news↔market driven by shared key_rate",
        "pass": (abs(r_news_rate) > 0.3 and abs(r_market_rate) > 0.2),
    }


def main() -> None:
    results = [check_S1(), check_S2(), check_S3(),
               check_S4(), check_S5(), check_S6()]
    print(json.dumps(results, indent=2, ensure_ascii=False))
    failed = [r for r in results if not r["pass"]]
    if failed:
        print(f"\n!!! {len(failed)} dataset(s) failed sanity check !!!")
        raise SystemExit(1)
    print("\nAll 6 datasets passed sanity check.")


if __name__ == "__main__":
    main()
