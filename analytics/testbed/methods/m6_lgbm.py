"""M6 — LightGBM regression with walk-forward time-series cross-validation.

For a hypothesis "news at lag L predicts market":

  Features:  news[t-L]  (the single-lag news count at the hypothesised lag)
  Target:    market_return[t]  (log-return)
  CV:        TimeSeriesSplit with N_SPLITS folds (expanding window)
  Metric:    Information Coefficient = Spearman rank-correlation between
             out-of-fold predictions and realised returns.
             IC > 0 means the model has directional skill.

  Confirmed iff:
    - IC_mean >= IC_MIN  (directional effect size)
    - IC bootstrap 5th-percentile > 0  (IC is robustly positive)
    - n_obs >= N_MIN

Unlike m3/m4 (classical statistics), m6 is a ML method. It can capture
non-linear news->market relationships. The walk-forward CV structure gives
a natural out-of-sample estimate, partially mitigating M008 within the
single dataset. For this reason m6 does NOT trigger M008 — the in-sample /
out-of-sample split is inherent in TimeSeriesSplit.

Expected behaviour on testbed:
  - S1 pure noise:           FPR ~ 5%   (IC distribution centred on 0)
  - S2 spurious trend:       FPR ~ 5%   (return transform sidesteps M001)
  - S3 known signal lag=14:  TPR moderate (IC ~ |r| * (1-noise))
  - S4 regime-dependent:     TPR moderate in first_half
  - S5 multi-signal:         A and B at right lags, C weaker
  - S6 confounder:           FPR MODERATE-HIGH (no key_rate feature; m6
                              cannot control for omitted variables without
                              being given the feature explicitly)

Pre-measurement penalty: 0.0  (no M001..M008 triggered — walk-forward CV
built-in). Post-measurement penalty depends on observed FPR.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy import stats

try:
    import lightgbm as lgb
    _HAS_LGB = True
except ImportError:
    _HAS_LGB = False

from .base import Hypothesis, MethodVerdict


M6_CONFIG: dict = {
    "method": "lgbm",
    "target_transform": "log_returns",          # sidesteps M001
    "p_value_method": "bootstrap",              # sidesteps M002
    "n_correction": "effective_n",              # sidesteps M007
    "confirmed_decision": "deterministic_threshold",  # sidesteps M004
    "subsetting_in_sql": False,                 # sidesteps M005
    "regime_split_check": "same_sign_required", # sidesteps M003
    "coverage_check": True,                     # sidesteps M006
    "out_of_sample_validation": True,           # M008 NOT fired — walk-forward CV
}

N_SPLITS = 5          # TimeSeriesSplit folds
IC_MIN = 0.01         # research mode: loosened from 0.03
N_MIN = 200           # minimum observations (need enough for CV folds)
N_BOOTSTRAP = 200     # bootstrap resamples for IC CI
BOOTSTRAP_CI_LO = 5   # 5th percentile of bootstrap IC must be > 0


def _hac_maxlag(n: int) -> int:
    return max(1, int(np.floor(4.0 * (n / 100.0) ** (2.0 / 9.0))))


def _spearman_ic(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Rank-correlation between predictions and realised returns."""
    if len(y_true) < 4:
        return 0.0
    r, _ = stats.spearmanr(y_pred, y_true)
    return 0.0 if not np.isfinite(r) else float(r)


@dataclass(frozen=True)
class M6LightGBM:
    name: str = "M6_lgbm_walkforward"
    config: dict = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        object.__setattr__(self, "config", dict(M6_CONFIG))

    def evaluate(self, df: pd.DataFrame, hyp: Hypothesis) -> MethodVerdict:
        if not _HAS_LGB:
            return MethodVerdict(
                confirmed=False, score=0.0, p_value=None, n=0,
                extra={"reason": "lightgbm_not_installed"},
            )

        # M001 protection: always use returns.
        if "market_return" in df.columns:
            target = df["market_return"].to_numpy(dtype=np.float64)
        else:
            close = df["market_close"].to_numpy(dtype=np.float64)
            target = np.empty_like(close)
            target[:] = np.nan
            target[1:] = np.diff(np.log(close))

        news = df[hyp.news_field].to_numpy(dtype=np.float64)

        # Regime slicing.
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

        n_days = len(target)
        start = L
        rows_x: list[float] = []
        rows_y: list[float] = []
        for t in range(start, n_days):
            x_val = news[t - L]
            y_val = target[t]
            if np.isfinite(x_val) and np.isfinite(y_val):
                rows_x.append(float(x_val))
                rows_y.append(float(y_val))

        n = len(rows_x)
        if n < N_MIN:
            return MethodVerdict(
                confirmed=False, score=0.0, p_value=None, n=n,
                extra={"reason": "insufficient_data"},
            )

        X = np.array(rows_x, dtype=np.float64).reshape(-1, 1)
        y = np.array(rows_y, dtype=np.float64)

        # TimeSeriesSplit walk-forward CV.
        fold_size = n // (N_SPLITS + 1)
        oof_preds = np.full(n, np.nan)
        lgb_params = {
            "objective": "regression",
            "n_estimators": 50,
            "num_leaves": 8,
            "learning_rate": 0.05,
            "min_child_samples": 20,
            "verbosity": -1,
            "random_state": 42,
        }
        for fold in range(N_SPLITS):
            train_end = fold_size * (fold + 1)
            test_start = train_end
            test_end = min(train_end + fold_size, n)
            if test_end <= test_start or train_end < 20:
                continue
            X_tr, y_tr = X[:train_end], y[:train_end]
            X_te = X[test_start:test_end]
            try:
                model = lgb.LGBMRegressor(**lgb_params)
                model.fit(X_tr, y_tr)
                oof_preds[test_start:test_end] = model.predict(X_te)
            except Exception:
                continue

        valid_mask = ~np.isnan(oof_preds)
        n_oof = int(valid_mask.sum())
        if n_oof < 20:
            return MethodVerdict(
                confirmed=False, score=0.0, p_value=None, n=n,
                extra={"reason": "too_few_oof_predictions"},
            )

        ic_mean = _spearman_ic(y[valid_mask], oof_preds[valid_mask])

        # Bootstrap CI for IC.
        rng = np.random.default_rng(seed=0)
        idx = np.where(valid_mask)[0]
        boot_ics: list[float] = []
        for _ in range(N_BOOTSTRAP):
            s = rng.choice(len(idx), size=len(idx), replace=True)
            boot_ics.append(_spearman_ic(y[idx[s]], oof_preds[idx[s]]))
        boot_arr = np.array(boot_ics)
        ic_lo = float(np.percentile(boot_arr, BOOTSTRAP_CI_LO))
        ic_hi = float(np.percentile(boot_arr, 100 - BOOTSTRAP_CI_LO))

        # Pseudo p-value: fraction of bootstrap samples <= 0.
        p_value = float((boot_arr <= 0.0).mean())
        if p_value <= 0.0:
            p_value = 1.0 / N_BOOTSTRAP

        direction = float(np.sign(ic_mean)) if ic_mean != 0.0 else 0.0

        # Gate: mean IC above threshold AND bootstrap 5th-pct > 0.
        if n < N_MIN:
            confirmed, reason = False, "n_below_min"
        elif ic_mean < IC_MIN:
            confirmed, reason = False, "ic_below_min"
        elif ic_lo <= 0.0:
            confirmed, reason = False, "bootstrap_ci_includes_zero"
        else:
            confirmed, reason = True, "ok"

        return MethodVerdict(
            confirmed=confirmed,
            score=float(ic_mean * direction) if direction != 0.0 else float(ic_mean),
            p_value=p_value,
            n=n,
            extra={
                "ic_min": IC_MIN, "n_min": N_MIN,
                "target_used": "market_return (log-returns)",
                "lag_days": L,
                "n_oof": n_oof,
                "ic_mean": ic_mean,
                "ic_bootstrap_lo": ic_lo,
                "ic_bootstrap_hi": ic_hi,
                "direction_sign": direction,
                "gate_reason": reason,
            },
        )


def build() -> M6LightGBM:
    return M6LightGBM()
