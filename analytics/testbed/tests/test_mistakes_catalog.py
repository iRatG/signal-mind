"""Unit tests for the mistakes catalogue — every matcher gets explicit cases."""
from __future__ import annotations

import pytest

from analytics.testbed.mistakes.catalog import (
    MISTAKES,
    _GOOD_CONFIG,
    _BAD_CONFIG_V1_STYLE,
    compute_penalty,
)


# ---------------------------------------------------------------------------
# Whole-catalogue assertions
# ---------------------------------------------------------------------------

def test_nine_mistakes_registered() -> None:
    assert len(MISTAKES) == 9
    ids = [m.id for m in MISTAKES]
    assert ids == [f"M{n:03d}" for n in range(1, 10)]


def test_all_weights_are_in_unit_range_or_above() -> None:
    """Sanity: no negative or huge weights."""
    for m in MISTAKES:
        assert 0.0 < m.weight <= 2.0


def test_good_config_yields_zero_penalty() -> None:
    total, triggered, _ = compute_penalty(_GOOD_CONFIG)
    assert total == 0.0
    assert triggered == []


def test_v1_style_config_triggers_exactly_seven_mistakes() -> None:
    """v1-style decision is via LLM (so M002 cannot fire), but M001/M003/
    M004/M005/M006/M007/M008 should all fire."""
    total, triggered, _ = compute_penalty(_BAD_CONFIG_V1_STYLE)
    assert set(triggered) == {"M001", "M003", "M004", "M005",
                              "M006", "M007", "M008"}
    assert total == pytest.approx(5.9, abs=1e-9)


# ---------------------------------------------------------------------------
# Per-matcher specific tests
# ---------------------------------------------------------------------------

def test_m001_only_fires_on_corr_and_levels() -> None:
    cfg = dict(_GOOD_CONFIG, target_transform="levels")
    _, triggered, _ = compute_penalty(cfg)
    assert triggered == ["M001"]

    # method is granger, target_transform is levels — should NOT fire M001
    cfg2 = dict(_GOOD_CONFIG, method="granger", target_transform="levels")
    _, t2, _ = compute_penalty(cfg2)
    assert "M001" not in t2


def test_m002_fires_only_on_threshold_without_hac() -> None:
    cfg = dict(_GOOD_CONFIG, p_value_method="classical")
    _, triggered, _ = compute_penalty(cfg)
    assert triggered == ["M002"]


def test_m003_fires_on_any_sign_acceptance() -> None:
    cfg = dict(_GOOD_CONFIG, regime_split_check="any_sign")
    _, triggered, _ = compute_penalty(cfg)
    assert triggered == ["M003"]


def test_m004_excludes_m002() -> None:
    """If LLM decides, the threshold-without-HAC story does not apply."""
    cfg = dict(_GOOD_CONFIG, confirmed_decision="llm")
    _, triggered, _ = compute_penalty(cfg)
    assert triggered == ["M004"]
    assert "M002" not in triggered


def test_m005_fires_on_subsetting() -> None:
    cfg = dict(_GOOD_CONFIG, subsetting_in_sql=True)
    _, triggered, _ = compute_penalty(cfg)
    assert triggered == ["M005"]


def test_m006_fires_on_missing_coverage_check() -> None:
    cfg = dict(_GOOD_CONFIG, coverage_check=False)
    _, triggered, _ = compute_penalty(cfg)
    assert triggered == ["M006"]


def test_m007_fires_on_raw_n() -> None:
    cfg = dict(_GOOD_CONFIG, n_correction="raw")
    _, triggered, _ = compute_penalty(cfg)
    assert triggered == ["M007"]


def test_m008_fires_when_no_walk_forward() -> None:
    cfg = dict(_GOOD_CONFIG, out_of_sample_validation=False)
    _, triggered, _ = compute_penalty(cfg)
    assert triggered == ["M008"]


def test_m009_fires_only_with_measured_fpr_over_10pct() -> None:
    # below threshold — does not fire
    cfg = dict(_GOOD_CONFIG, measured_fpr_on_shuffle=0.09)
    _, triggered, _ = compute_penalty(cfg)
    assert triggered == []
    # exactly at threshold — does not fire
    cfg = dict(_GOOD_CONFIG, measured_fpr_on_shuffle=0.10)
    _, triggered, _ = compute_penalty(cfg)
    assert triggered == []
    # above threshold — fires
    cfg = dict(_GOOD_CONFIG, measured_fpr_on_shuffle=0.11)
    _, triggered, _ = compute_penalty(cfg)
    assert triggered == ["M009"]
    # None — does not fire (pre-measurement state)
    cfg = dict(_GOOD_CONFIG, measured_fpr_on_shuffle=None)
    _, triggered, _ = compute_penalty(cfg)
    assert triggered == []


def test_breakdown_dictionary_matches_triggered_list() -> None:
    _, triggered, breakdown = compute_penalty(_BAD_CONFIG_V1_STYLE)
    assert set(breakdown.keys()) == set(triggered)
