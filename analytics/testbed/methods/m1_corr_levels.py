"""M1 — v1-style baseline.

Pearson correlation on price LEVELS (not returns) with the v1 frozen
thresholds: |r| >= 0.4 AND n >= 150 AND p < 0.01 (classical t-test).

This is intentionally the WORST method — it embodies mistakes M001, M002,
M006, M007, M008 of the catalogue (pre-measurement penalty 4.1). Phase A.1
runs it on the testbed to quantify how broken it is.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd
from scipy import stats

from .base import Hypothesis, MethodVerdict, shift_forward


M1_CONFIG: dict = {
    "method": "pearson_corr",
    "target_transform": "levels",
    "p_value_method": "classical",
    "n_correction": "raw",
    "confirmed_decision": "deterministic_threshold",
    "subsetting_in_sql": False,
    "regime_split_check": "not_applicable",
    "coverage_check": False,
    "out_of_sample_validation": False,
}

# Thresholds copied verbatim from analytics/experiment_v1.md frozen design.
R_MIN = 0.40
N_MIN = 150
P_MAX = 0.01


@dataclass(frozen=True)
class M1CorrLevels:
    """Pearson r on LEVELS — v1 baseline."""
    name: str = "M1_corr_levels"
    config: dict = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        # Frozen-default config trick (dataclass + frozen)
        object.__setattr__(self, "config", dict(M1_CONFIG))

    def evaluate(self, df: pd.DataFrame, hyp: Hypothesis) -> MethodVerdict:
        # Resolve target on LEVELS (key v1 mistake — even if dataframe carries
        # returns, we explicitly take a level-style column when present).
        if "market_close" in df.columns:
            target = df["market_close"].to_numpy(dtype=np.float64)
        else:
            target = df[hyp.target_field].to_numpy(dtype=np.float64)

        news = df[hyp.news_field].to_numpy(dtype=np.float64)
        news_lagged = shift_forward(news, hyp.lag_days)

        # Optional regime filter (used by S4, harness passes hyp.regime_filter)
        if hyp.regime_filter == "first_half":
            half = len(df) // 2
            news_lagged = news_lagged[:half]
            target = target[:half]
        elif hyp.regime_filter == "second_half":
            half = len(df) // 2
            news_lagged = news_lagged[half:]
            target = target[half:]

        mask = ~np.isnan(news_lagged) & ~np.isnan(target)
        n = int(mask.sum())
        if n < 3:
            return MethodVerdict(
                confirmed=False, score=0.0, p_value=None, n=n,
                extra={"reason": "insufficient_data"},
            )

        x = news_lagged[mask]
        y = target[mask]
        r, p = stats.pearsonr(x, y)

        confirmed = (abs(r) >= R_MIN) and (n >= N_MIN) and (p < P_MAX)
        return MethodVerdict(
            confirmed=bool(confirmed),
            score=float(r),
            p_value=float(p),
            n=n,
            extra={
                "r_min": R_MIN, "n_min": N_MIN, "p_max": P_MAX,
                "target_used": "market_close (levels)",
            },
        )


def build() -> M1CorrLevels:
    return M1CorrLevels()
