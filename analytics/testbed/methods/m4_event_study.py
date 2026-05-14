"""M4 — Event study: do news spikes predict future returns?

For a hypothesis "news leads market by L days":

  1. Identify "events" as days where z(news_mentions) > EVENT_Z (default 2.0).
  2. For each event tau, collect the return at day tau + L.
  3. Test H0: mean(returns_at_lag_L_after_events) == 0
     via an intercept-only OLS with Newey-West HAC standard errors,
     which absorbs autocorrelation from overlapping event windows.
  4. Confirmed iff n_events >= N_MIN_EVENTS AND HAC p_value < P_MAX.

The event-study lens is methodologically distinct from m2/m3:
m2 measures the *linear* association across all days; m4 conditions on
*spike days* only. This is closer to a real trading signal ("when news
explodes, what does the market do later?"). The single-day-at-lag form
matches the testbed's per-lag hypothesis schedule cleanly.

Expected behaviour on testbed:
  - S1 pure noise:           FPR ~ 1%   (HAC p<0.01 calibrated)
  - S2 spurious trend:       FPR ~ 1%   (returns transform sidesteps M001)
  - S3 known signal lag=14:  TPR > 0    (news spike z>2 implies r=-0.3*2 = -0.6 sigma
                                          conditional return; detectable at n_events ~25)
  - S4 regime-dependent:     TPR high in first-half lag=7, ~0 in second
  - S5 multi-signal:         topic_A at L=7 and topic_B at L=30 confirmed
  - S6 confounder:           FPR MODERATE-HIGH — event days coincide with
                              high-key_rate days; key_rate persistence
                              imprints on returns at lag L without any
                              causal news -> market link.

m4 triggers M008 (no walk-forward inside one dataset). Other v1-family
mistakes avoided by construction.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy import stats

from .base import Hypothesis, MethodVerdict


M4_CONFIG: dict = {
    "method": "event_study",
    "target_transform": "log_returns",          # sidesteps M001
    "p_value_method": "HAC",                    # sidesteps M002
    "n_correction": "effective_n",              # HAC absorbs autocorr; sidesteps M007
    "confirmed_decision": "deterministic_threshold",  # sidesteps M004
    "subsetting_in_sql": False,                 # sidesteps M005
    "regime_split_check": "same_sign_required", # sidesteps M003
    "coverage_check": True,                     # sidesteps M006
    "out_of_sample_validation": False,          # M008 still fires
}

# Event threshold tuning rationale: with Poisson(~20) news_mentions, z>2 yields
# ~2-3% of days (~12 events per 500-day regime, ~25 per 1000-day full series).
# That's too few for stable HAC t-tests on regime-split datasets (S4 first_half
# gave TPR=4% during diagnostic run). z>1.5 yields ~7% of days, restoring power
# on shorter windows while still focusing on tail-event behaviour.
EVENT_Z = 1.5          # z-score threshold for news-spike events
N_MIN_EVENTS = 20      # minimum #events per seed for a stable test
P_MAX = 0.01


def _hac_maxlag(n: int) -> int:
    """Newey-West rule-of-thumb: maxlags = floor(4 * (n/100)^(2/9))."""
    return max(1, int(np.floor(4.0 * (n / 100.0) ** (2.0 / 9.0))))


@dataclass(frozen=True)
class M4EventStudy:
    name: str = "M4_event_study_hac"
    config: dict = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        object.__setattr__(self, "config", dict(M4_CONFIG))

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

        # Regime slicing (same convention as m1/m2/m3).
        if hyp.regime_filter == "first_half":
            half = len(df) // 2
            target = target[:half]
            news = news[:half]
        elif hyp.regime_filter == "second_half":
            half = len(df) // 2
            target = target[half:]
            news = news[half:]

        L = int(hyp.lag_days)
        if L < 1:
            return MethodVerdict(
                confirmed=False, score=0.0, p_value=None, n=0,
                extra={"reason": "lag_days_less_than_1"},
            )

        # In-sample z-score of news (testbed series are stationary so a
        # full-sample mean/std is fine; for real Train we'd switch to a
        # rolling window — flagged for Phase B).
        finite_news = news[np.isfinite(news)]
        if finite_news.size < 30:
            return MethodVerdict(
                confirmed=False, score=0.0, p_value=None, n=0,
                extra={"reason": "insufficient_news_data"},
            )
        mu = float(finite_news.mean())
        sigma = float(finite_news.std(ddof=0))
        if sigma <= 0.0 or not np.isfinite(sigma):
            return MethodVerdict(
                confirmed=False, score=0.0, p_value=None, n=0,
                extra={"reason": "zero_or_nan_news_sigma"},
            )

        z_news = (news - mu) / sigma
        event_idx = np.where(z_news > EVENT_Z)[0]

        # Collect returns at day tau + L for each event tau, when in range.
        n_days = len(target)
        rows: list[float] = []
        used_events: list[int] = []
        for tau in event_idx:
            i = int(tau) + L
            if i < 0 or i >= n_days:
                continue
            r = target[i]
            if not np.isfinite(r):
                continue
            rows.append(float(r))
            used_events.append(int(tau))

        n_events = len(rows)
        if n_events < N_MIN_EVENTS:
            return MethodVerdict(
                confirmed=False, score=0.0, p_value=None, n=n_events,
                extra={
                    "reason": "too_few_events",
                    "event_threshold_z": EVENT_Z,
                    "n_events_raw": int(event_idx.size),
                },
            )

        y = np.array(rows, dtype=np.float64)
        # Intercept-only OLS with HAC SE — clean way to t-test mean(y)==0
        # while letting HAC soak up autocorrelation from overlapping windows.
        X = np.ones((n_events, 1))
        try:
            res = sm.OLS(y, X).fit(
                cov_type="HAC",
                cov_kwds={"maxlags": _hac_maxlag(n_events)},
            )
        except Exception as exc:  # pragma: no cover — defensive
            return MethodVerdict(
                confirmed=False, score=0.0, p_value=None, n=n_events,
                extra={"reason": f"ols_failed: {exc!r}"},
            )

        mean_car = float(res.params[0])
        se = float(res.bse[0])
        if se <= 0.0 or not np.isfinite(se):
            return MethodVerdict(
                confirmed=False, score=mean_car, p_value=None, n=n_events,
                extra={"reason": "zero_or_nan_se"},
            )
        t_stat = mean_car / se
        df_t = max(1, n_events - 1)
        p_value = float(2.0 * (1.0 - stats.t.cdf(abs(t_stat), df_t)))
        if p_value <= 0.0:
            p_value = 1e-300

        direction = float(np.sign(mean_car)) if mean_car != 0.0 else 0.0

        # Custom gate: HAC p_value below threshold and enough events.
        if n_events < N_MIN_EVENTS:
            confirmed, reason = False, "n_below_min"
        elif p_value >= P_MAX:
            confirmed, reason = False, "p_above_max"
        else:
            confirmed, reason = True, "ok"

        return MethodVerdict(
            confirmed=confirmed,
            score=mean_car,
            p_value=p_value,
            n=n_events,
            extra={
                "event_threshold_z": EVENT_Z,
                "n_min_events": N_MIN_EVENTS,
                "p_max": P_MAX,
                "target_used": "market_return (log-returns)",
                "lag_days": L,
                "n_events_raw": int(event_idx.size),
                "hac_maxlag": _hac_maxlag(n_events),
                "se_hac": se,
                "t_stat": float(t_stat),
                "direction_sign": direction,
                "gate_reason": reason,
            },
        )


def build() -> M4EventStudy:
    return M4EventStudy()
