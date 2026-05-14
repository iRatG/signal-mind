"""M3 — Granger-style single-lag F-test (news -> market) on returns with HAC.

For a hypothesis "news at lag L predicts market", fit

    y[t] = c + sum_{i=1..p_ar} a_i * y[t-i] + b * news[t-L] + e

with Newey-West HAC standard errors, then test H0: b == 0 via a Wald F-test.
This is the single-lag analogue of the classical Granger sweep; it matches
the testbed's per-lag hypothesis schedule (each lag is its own hypothesis,
so we deliberately avoid the multi-lag formulation in which a window at
lag>=L_true would always subsume the true signal and inflate FPR).

The AR control lets m3 separate news's incremental predictive content from
own-history persistence — that is the structural improvement over m2.

Expected behaviour on testbed:
  - S1 pure noise:           FPR ~ 1-5%  (HAC + p<0.01)
  - S2 spurious trend:       FPR ~ 1-5%  (returns + HAC sidesteps M001/M002)
  - S3 known signal lag=14:  TPR high at L=14, ~0 at other lags (m2 had 0%)
  - S4 regime-dependent:     TPR high in first-half lag=7, ~0 elsewhere
  - S5 multi-signal:         topic_A confirmed at L=7, topic_B at L=30,
                              topic_C rejected at all L (and A/B rejected
                              at wrong lags)
  - S6 confounder:           FPR HIGH — Granger is a statistical, not
                              causal, test. m5 (VAR with key_rate) is
                              the intended fix for confounder discipline.

m3 still triggers M008 (no walk-forward inside one dataset). All other
v1-family mistakes are avoided by construction.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
import statsmodels.api as sm

from .base import Hypothesis, MethodVerdict


M3_CONFIG: dict = {
    "method": "granger",
    "target_transform": "log_returns",          # sidesteps M001
    "p_value_method": "HAC",                    # sidesteps M002
    "n_correction": "effective_n",              # HAC absorbs autocorr; sidesteps M007
    "confirmed_decision": "deterministic_threshold",  # sidesteps M004
    "subsetting_in_sql": False,                 # sidesteps M005
    "regime_split_check": "same_sign_required", # sidesteps M003
    "coverage_check": True,                     # sidesteps M006
    "out_of_sample_validation": False,          # M008 still fires inside one dataset
}

# AR control order: small fixed lag for parsimony. 5 trading days ~= 1 week
# of own-history conditioning; enough to soak up short-run return autocorr
# without over-fitting.
AR_ORDER = 5

# Gate thresholds. F is essentially t^2 for 1 restriction, so F_MIN is a
# sanity floor; the binding constraint is p<P_MAX.
F_MIN = 1.0
N_MIN = 150
P_MAX = 0.01


def _hac_maxlag(n: int) -> int:
    """Newey-West rule-of-thumb: maxlags = floor(4 * (n/100)^(2/9))."""
    return max(1, int(np.floor(4.0 * (n / 100.0) ** (2.0 / 9.0))))


def _build_design(
    y: np.ndarray, news: np.ndarray, ar_p: int, news_lag: int
) -> tuple[np.ndarray, np.ndarray] | None:
    """Build the single-lag Granger design:

        y[t] = const + sum_{i=1..ar_p} a_i * y[t-i] + b * news[t-news_lag]

    Returns (Y, X) ready for sm.OLS (without constant — caller adds it),
    or None if not enough finite rows survive.
    """
    n = len(y)
    start = max(ar_p, news_lag)
    if n <= start:
        return None
    Y_rows: list[float] = []
    X_rows: list[np.ndarray] = []
    for t in range(start, n):
        # y[t-1], y[t-2], ..., y[t-ar_p]
        block_y = y[t - ar_p:t][::-1]
        news_val = news[t - news_lag]
        if (not np.isfinite(y[t])
                or not np.isfinite(block_y).all()
                or not np.isfinite(news_val)):
            continue
        Y_rows.append(float(y[t]))
        X_rows.append(np.concatenate([block_y, [news_val]]))
    if len(Y_rows) < 30:
        return None
    Y = np.array(Y_rows, dtype=np.float64)
    X = np.vstack(X_rows).astype(np.float64)
    return Y, X


@dataclass(frozen=True)
class M3Granger:
    name: str = "M3_granger_hac"
    config: dict = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        object.__setattr__(self, "config", dict(M3_CONFIG))

    def evaluate(self, df: pd.DataFrame, hyp: Hypothesis) -> MethodVerdict:
        # Structural M001 protection: always evaluate on returns.
        if "market_return" in df.columns:
            target = df["market_return"].to_numpy(dtype=np.float64)
        else:
            close = df["market_close"].to_numpy(dtype=np.float64)
            target = np.empty_like(close)
            target[:] = np.nan
            target[1:] = np.diff(np.log(close))

        news = df[hyp.news_field].to_numpy(dtype=np.float64)

        # Regime slicing (same convention as m1/m2).
        if hyp.regime_filter == "first_half":
            half = len(df) // 2
            target = target[:half]
            news = news[:half]
        elif hyp.regime_filter == "second_half":
            half = len(df) // 2
            target = target[half:]
            news = news[half:]

        news_lag = int(hyp.lag_days)
        if news_lag < 1:
            return MethodVerdict(
                confirmed=False, score=0.0, p_value=None, n=0,
                extra={"reason": "lag_days_less_than_1"},
            )

        built = _build_design(target, news, AR_ORDER, news_lag)
        if built is None:
            return MethodVerdict(
                confirmed=False, score=0.0, p_value=None, n=0,
                extra={"reason": "insufficient_data"},
            )
        Y, X = built
        n = len(Y)
        X_const = sm.add_constant(X)

        try:
            res = sm.OLS(Y, X_const).fit(
                cov_type="HAC",
                cov_kwds={"maxlags": _hac_maxlag(n)},
            )
        except Exception as exc:  # pragma: no cover — defensive
            return MethodVerdict(
                confirmed=False, score=0.0, p_value=None, n=n,
                extra={"reason": f"ols_failed: {exc!r}"},
            )

        # News coefficient is the last column (after const + AR lags).
        news_idx = 1 + AR_ORDER
        b = float(res.params[news_idx])
        # Wald F-test on b == 0 (1 restriction; F = t^2).
        R = np.zeros((1, X_const.shape[1]))
        R[0, news_idx] = 1.0
        try:
            wald = res.wald_test(R, use_f=True, scalar=True)
            F_stat = float(wald.statistic)
            p_value = float(wald.pvalue)
        except Exception as exc:  # pragma: no cover — defensive
            return MethodVerdict(
                confirmed=False, score=0.0, p_value=None, n=n,
                extra={"reason": f"wald_failed: {exc!r}"},
            )
        if not np.isfinite(F_stat) or not np.isfinite(p_value):
            return MethodVerdict(
                confirmed=False,
                score=F_stat if np.isfinite(F_stat) else 0.0,
                p_value=None, n=n,
                extra={"reason": "nonfinite_wald"},
            )

        direction = float(np.sign(b)) if b != 0.0 else 0.0

        # Custom gate (Granger uses F, not r — evaluator gate doesn't fit).
        if F_stat < F_MIN:
            confirmed, reason = False, "F_below_min"
        elif n < N_MIN:
            confirmed, reason = False, "n_below_min"
        elif p_value >= P_MAX:
            confirmed, reason = False, "p_above_max"
        else:
            confirmed, reason = True, "ok"

        return MethodVerdict(
            confirmed=confirmed,
            score=float(F_stat * direction) if direction != 0.0 else float(F_stat),
            p_value=p_value,
            n=n,
            extra={
                "f_min": F_MIN, "n_min": N_MIN, "p_max": P_MAX,
                "target_used": "market_return (log-returns)",
                "ar_order": AR_ORDER, "news_lag": news_lag,
                "hac_maxlag": _hac_maxlag(n),
                "F_stat": F_stat,
                "b_news": b,
                "direction_sign": direction,
                "gate_reason": reason,
            },
        )


def build() -> M3Granger:
    return M3Granger()
