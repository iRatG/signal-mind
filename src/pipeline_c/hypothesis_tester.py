"""Phase C — Hypothesis Tester.

Takes a RagHypothesis, runs M5+M6 on Train, then Val (and optionally Test).
Reuses the exact same infrastructure as Phase B (feature_transformer, M5, M6).

Usage:
    from src.pipeline_c.hypothesis_tester import HypothesisTester
    tester = HypothesisTester()
    results = tester.test(hypothesis)
"""
from __future__ import annotations

import importlib
import sys
import warnings
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.pipeline_c.hypothesis_schema import RagHypothesis, HypothesisResult, MOEX_SECTOR_COLS
from src.pipeline_v2.train_scanner import load_split
from src.pipeline_v2.big_scanner import load_moex_split
from src.pipeline_v2.feature_transformer import FeatureTransformer

M5_MODULE = "analytics.testbed.methods.m5_var"
M6_MODULE = "analytics.testbed.methods.m6_lgbm"

N_MIN = 80   # minimum observations for a test to be valid


class HypothesisTester:
    """Run M5+M6 ensemble on a single RagHypothesis across splits."""

    def __init__(self) -> None:
        self._m5 = None
        self._m6 = None

    def _load_methods(self) -> None:
        if self._m5 is None:
            self._m5 = importlib.import_module(M5_MODULE).build()
        if self._m6 is None:
            self._m6 = importlib.import_module(M6_MODULE).build()

    # ──────────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────────

    def test(
        self,
        hyp: RagHypothesis,
        splits: tuple[str, ...] = ("train", "val"),
        verbose: bool = False,
    ) -> list[HypothesisResult]:
        """Test hypothesis on given splits. Returns one HypothesisResult per (split, lag).

        After calling, hyp.tested = True and hyp.results is populated.
        """
        self._load_methods()
        ok, reason = hyp.is_valid()
        if not ok:
            return [_error_result(hyp, 0, "train", f"invalid hypothesis: {reason}")]

        all_results: list[HypothesisResult] = []
        train_results: list[HypothesisResult] = []

        for split in splits:
            df = _load_any_split(split, hyp.instrument)
            if df is None:
                if verbose:
                    print(f"    No {split} data for {hyp.instrument}")
                continue

            df = _apply_feature(df, hyp.feature_type)
            if df is None:
                if verbose:
                    print(f"    Feature apply failed for {hyp.feature_type}")
                continue

            topic_col = _topic_col(hyp.topic, hyp.feature_type)
            if topic_col not in df.columns:
                if verbose:
                    print(f"    Column {topic_col} not found in {split} data")
                continue

            for lag in hyp.lag_days:
                res = self._run_one(hyp, df, topic_col, lag, split, verbose)
                all_results.append(res)
                if split == "train":
                    train_results.append(res)

        # Flag sign flips: val direction vs train direction
        _annotate_sign_flips(train_results, all_results)

        hyp.tested  = True
        hyp.results = [r.to_dict() for r in all_results]
        return all_results

    # ──────────────────────────────────────────────────────────────────────────
    # Internal
    # ──────────────────────────────────────────────────────────────────────────

    def _run_one(
        self,
        hyp: RagHypothesis,
        df: pd.DataFrame,
        topic_col: str,
        lag: int,
        split: str,
        verbose: bool,
    ) -> HypothesisResult:
        from analytics.testbed.methods.base import Hypothesis as TestHyp

        test_hyp = TestHyp(
            news_field   = topic_col,
            target_field = "market_return",
            lag_days     = lag,
        )

        n_obs = len(df.dropna(subset=[topic_col, "market_return"]))
        if n_obs < N_MIN:
            return _error_result(hyp, lag, split, f"n={n_obs} < {N_MIN}")

        try:
            v5 = self._m5.evaluate(df, test_hyp)
        except Exception as e:
            return _error_result(hyp, lag, split, f"M5 error: {e}")

        try:
            v6 = self._m6.evaluate(df, test_hyp)
        except Exception as e:
            return _error_result(hyp, lag, split, f"M6 error: {e}")

        ensemble_pass = v5.confirmed and v6.confirmed

        res = HypothesisResult(
            hypothesis_id  = hyp.hypothesis_id,
            instrument     = hyp.instrument,
            topic          = hyp.topic,
            lag            = lag,
            split          = split,
            m5_confirmed   = bool(v5.confirmed),
            m5_pvalue      = round(float(v5.p_value or 1.0), 6),
            m5_score       = round(float(v5.score), 6),
            m6_confirmed   = bool(v6.confirmed),
            m6_ic          = round(float(v6.score), 6),
            m6_pvalue      = round(float(v6.p_value or 1.0), 6),
            n_obs          = n_obs,
            ensemble_pass  = ensemble_pass,
            source_label   = hyp.source_label(),
        )

        if verbose:
            status = "PASS" if ensemble_pass else "fail"
            print(
                f"    [{split}] {status}  lag={lag:3d}  "
                f"m5={'Y' if v5.confirmed else 'n'} p={res.m5_pvalue:.3f}  "
                f"m6={'Y' if v6.confirmed else 'n'} ic={res.m6_ic:.4f}  "
                f"n={n_obs}"
            )
        return res


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _load_any_split(split: str, instrument: str) -> Optional[pd.DataFrame]:
    """Load market+news data for any instrument — international or MOEX sector."""
    moex_col = MOEX_SECTOR_COLS.get(instrument)
    if moex_col:
        return load_moex_split(split, moex_col)
    return load_split(split, instrument)


def _apply_feature(df: pd.DataFrame, feature_type: str) -> Optional[pd.DataFrame]:
    try:
        return FeatureTransformer.apply(df, feature_type, "market_return")
    except Exception:
        return None


def _topic_col(topic: str, feature_type: str) -> str:
    """Map topic + feature_type to actual column name."""
    if feature_type == "keyword_z90":
        return f"{topic}_z"
    if feature_type == "embedding_z90":
        return f"{topic}_emb_z"
    return topic


def _error_result(hyp: RagHypothesis, lag: int, split: str, error: str) -> HypothesisResult:
    return HypothesisResult(
        hypothesis_id = hyp.hypothesis_id,
        instrument    = hyp.instrument,
        topic         = hyp.topic,
        lag           = lag,
        split         = split,
        error         = error,
        source_label  = hyp.source_label(),
    )


def _annotate_sign_flips(
    train_results: list[HypothesisResult],
    all_results: list[HypothesisResult],
) -> None:
    """Mark val/test results where IC sign flips vs train."""
    train_signs: dict[int, float] = {
        r.lag: r.m6_ic for r in train_results if r.ensemble_pass
    }
    for r in all_results:
        if r.split != "train" and r.lag in train_signs:
            train_ic = train_signs[r.lag]
            if train_ic != 0 and r.m6_ic != 0:
                r.sign_flip = (train_ic > 0) != (r.m6_ic > 0)


# ──────────────────────────────────────────────────────────────────────────────
# Batch convenience
# ──────────────────────────────────────────────────────────────────────────────

def test_batch(
    hypotheses: list[RagHypothesis],
    splits: tuple[str, ...] = ("train", "val"),
    verbose: bool = True,
) -> list[HypothesisResult]:
    """Test a list of hypotheses. Returns all results flat."""
    tester = HypothesisTester()
    all_results: list[HypothesisResult] = []
    for i, hyp in enumerate(hypotheses):
        if verbose:
            print(f"  [{i+1}/{len(hypotheses)}] {hyp.hypothesis_id}  "
                  f"{hyp.instrument}/{hyp.topic}  src={hyp.source_label()}")
        results = tester.test(hyp, splits=splits, verbose=verbose)
        all_results.extend(results)
    return all_results
