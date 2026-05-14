"""Mistakes catalogue — known methodological errors of the project.

Each mistake is a (pattern, weight, source) triple. Patterns are Python
predicates over a `config: dict` describing a candidate ensemble setup.
The penalty term in the ensemble Loss function sums weights of all
triggered mistakes:

    Loss(config) = alpha * FPR + beta * (1 - TPR)
                 + gamma * sum_i (w_i * 1[m_i.matches(config)])
                 + delta * complexity(config)

This makes the testbed an Ouroboros-style learning system: every time the
project discovers a new failure mode, we add a record here, and the next
ensemble selection automatically penalises that mistake.

Single source of truth: this file.
DuckDB export: `catalog.duckdb` (runtime artefact, gitignored, regenerable).
Markdown export: `catalog.md` (committed, human-readable mirror).

Run as a module:
    python -m analytics.testbed.mistakes.catalog
to (re)create the duckdb, refresh the .md, and run self-tests.

Phase A.0b of v2 testbed-first methodology.
"""
from __future__ import annotations

import io
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import duckdb

# Force UTF-8 stdout on Windows (cp1251 default mangles Cyrillic / unicode).
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                  line_buffering=True)

HERE = Path(__file__).resolve().parent
DB_PATH = HERE / "catalog.duckdb"
MD_PATH = HERE / "catalog.md"


# ---------------------------------------------------------------------------
# Config schema (for matcher functions to operate on)
# ---------------------------------------------------------------------------
# A `config` dict describes one candidate ensemble configuration. Schema:
#
#   method:               "pearson_corr" | "granger" | "event_study"
#                         | "var" | "lgbm" | "ensemble"
#   target_transform:     "levels" | "returns" | "log_returns" | "diff"
#   p_value_method:       "classical" | "HAC" | "newey_west" | "bootstrap"
#   n_correction:         "raw" | "effective_n" | "block_bootstrap"
#   confirmed_decision:   "llm" | "deterministic_threshold"
#   subsetting_in_sql:    bool  — True if SQL uses arbitrary WHERE thresholds
#   regime_split_check:   "any_sign" | "same_sign_required" | "not_applicable"
#   coverage_check:       bool  — True if topics with coverage <30% are filtered
#   out_of_sample_validation: bool  — True if Train/Val/Test walk-forward used
#   measured_fpr_on_shuffle:  float  — runtime, populated after A.5 shuffle run
#
# Defaults assume the WORST case (no protections) — that way an
# under-specified config is treated as risky.

CONFIG_KEYS = (
    "method",
    "target_transform",
    "p_value_method",
    "n_correction",
    "confirmed_decision",
    "subsetting_in_sql",
    "regime_split_check",
    "coverage_check",
    "out_of_sample_validation",
    "measured_fpr_on_shuffle",
)


@dataclass(frozen=True)
class Mistake:
    """A pattern of methodological error with a penalty weight."""
    id: str
    description: str
    pattern_summary: str
    weight: float
    source: str
    matcher: Callable[[dict], bool] = field(repr=False)


# ---------------------------------------------------------------------------
# Matcher predicates — keep them small, pure, side-effect free
# ---------------------------------------------------------------------------

def _is_corr_family(cfg: dict) -> bool:
    return cfg.get("method") in {"pearson_corr", "corr", "spearman"}


def _m001_corr_on_levels(cfg: dict) -> bool:
    """CORR / Pearson r computed on price levels rather than returns."""
    return _is_corr_family(cfg) and cfg.get("target_transform") == "levels"


def _m002_confirmed_without_hac(cfg: dict) -> bool:
    """Confirmed by significance, but p-value used was classical (no HAC)."""
    if cfg.get("confirmed_decision") != "deterministic_threshold":
        return False
    return cfg.get("p_value_method") not in {"HAC", "newey_west", "bootstrap"}


def _m003_mixed_regime_accepted(cfg: dict) -> bool:
    """Multi-regime result with opposite signs counted as confirmed."""
    return cfg.get("regime_split_check") == "any_sign"


def _m004_llm_decides_confirmed(cfg: dict) -> bool:
    """LLM emits its own confirmed=true/false instead of deterministic gate."""
    return cfg.get("confirmed_decision") == "llm"


def _m005_arbitrary_subsetting(cfg: dict) -> bool:
    """SQL uses WHERE thresholds that were not pre-registered."""
    return bool(cfg.get("subsetting_in_sql", False))


def _m006_low_coverage_topic(cfg: dict) -> bool:
    """Topic with coverage <30% in the window admitted to analysis."""
    return not cfg.get("coverage_check", False)


def _m007_raw_n_no_autocorr(cfg: dict) -> bool:
    """n used in significance without effective-n / autocorr correction."""
    return cfg.get("n_correction") == "raw"


def _m008_no_walk_forward(cfg: dict) -> bool:
    """No out-of-sample validation — sign(r) flip on holdout untestable."""
    return not cfg.get("out_of_sample_validation", False)


def _m009_high_fpr_on_shuffle(cfg: dict) -> bool:
    """Pipeline returns >10% confirmed on shuffled labels (measured)."""
    measured = cfg.get("measured_fpr_on_shuffle")
    return measured is not None and measured > 0.10


# ---------------------------------------------------------------------------
# Seed catalogue — frozen as of 2026-05-14
# ---------------------------------------------------------------------------

MISTAKES: tuple[Mistake, ...] = (
    Mistake(
        id="M001",
        description=(
            "CORR / Pearson r computed on price levels instead of returns. "
            "Two trending series will show spurious high |r| without any "
            "predictive content (Granger 1974 spurious regression)."
        ),
        pattern_summary="method in {corr,...} AND target_transform == 'levels'",
        weight=1.0,
        source="DS audit 2026-05-14; Marathon 2/5; smoke v1",
        matcher=_m001_corr_on_levels,
    ),
    Mistake(
        id="M002",
        description=(
            "Confirmed by p-value, but the p-value uses the classical formula "
            "for i.i.d. samples — invalid for autocorrelated time series. "
            "Newey-West HAC or block bootstrap is required."
        ),
        pattern_summary="threshold-based confirm AND p_value_method != HAC/bootstrap",
        weight=0.8,
        source="DS audit 2026-05-14",
        matcher=_m002_confirmed_without_hac,
    ),
    Mistake(
        id="M003",
        description=(
            "Multi-regime result with opposite r signs across regimes was "
            "counted as confirmed. Real example: smoke v1 had r=-0.34 in "
            "high-inflation regime and r=+0.58 in low — confirmed=true."
        ),
        pattern_summary="regime_split_check == 'any_sign'",
        weight=1.0,
        source="smoke v1 inflation case",
        matcher=_m003_mixed_regime_accepted,
    ),
    Mistake(
        id="M004",
        description=(
            "LLM emits its own confirmed=true/false and signal_score, rather "
            "than the pipeline applying a deterministic statistical gate."
        ),
        pattern_summary="confirmed_decision == 'llm'",
        weight=0.9,
        source="Marathon 1-5; all smoke runs",
        matcher=_m004_llm_decides_confirmed,
    ),
    Mistake(
        id="M005",
        description=(
            "SQL contains arbitrary WHERE filters (volatility quantiles, "
            "ad-hoc thresholds) that were not declared in the pre-registered "
            "hypothesis. Each such filter inflates the multiple-testing burden."
        ),
        pattern_summary="subsetting_in_sql == True",
        weight=0.7,
        source="smoke v1: WHERE daily_vol <= 3",
        matcher=_m005_arbitrary_subsetting,
    ),
    Mistake(
        id="M006",
        description=(
            "Topic with <30% non-null coverage in the active window admitted "
            "to analysis as a full-coverage feature. Reduces effective n in "
            "ways that classical n_min checks don't see."
        ),
        pattern_summary="coverage_check is missing/False",
        weight=0.6,
        source="data_audit_v1: ruble, sanctions",
        matcher=_m006_low_coverage_topic,
    ),
    Mistake(
        id="M007",
        description=(
            "Raw sample size n used in significance — no correction for "
            "autocorrelation. For autocorrelated returns, effective n is "
            "much smaller, and p-values are overstated."
        ),
        pattern_summary="n_correction == 'raw'",
        weight=0.8,
        source="DS audit 2026-05-14",
        matcher=_m007_raw_n_no_autocorr,
    ),
    Mistake(
        id="M008",
        description=(
            "No walk-forward / out-of-sample validation. sign(r) reversal "
            "between Discovery and Validation cannot be detected (historical "
            "examples: USD/RUB->MOEXFN, Brent->MOEXFN failed holdout 2026-05-02)."
        ),
        pattern_summary="out_of_sample_validation == False",
        weight=0.9,
        source="holdout 2026-05-02",
        matcher=_m008_no_walk_forward,
    ),
    Mistake(
        id="M009",
        description=(
            "Pipeline returns >10% confirmed rate on shuffled labels. "
            "Indicates the test itself is the source of confirmations, not "
            "any real structure in data. Measured during Phase A.5."
        ),
        pattern_summary="measured_fpr_on_shuffle > 0.10",
        weight=1.0,
        source="Phase A.5 measurement (TBD)",
        matcher=_m009_high_fpr_on_shuffle,
    ),
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_penalty(config: dict) -> tuple[float, list[str], dict[str, float]]:
    """Sum penalty weights of all mistakes that match the given config.

    Returns:
        total: float                 — sum of weights of triggered mistakes
        triggered: list[str]         — IDs of mistakes that matched
        breakdown: dict[str, float]  — id -> weight for triggered ones
    """
    total = 0.0
    triggered: list[str] = []
    breakdown: dict[str, float] = {}
    for m in MISTAKES:
        if m.matcher(config):
            total += m.weight
            triggered.append(m.id)
            breakdown[m.id] = m.weight
    return total, triggered, breakdown


def write_to_duckdb(db_path: Path = DB_PATH) -> None:
    """Recreate `mistakes_catalog` table in DuckDB and seed it with MISTAKES."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        con.execute("DROP TABLE IF EXISTS mistakes_catalog")
        con.execute(
            """
            CREATE TABLE mistakes_catalog (
                id              VARCHAR PRIMARY KEY,
                description     TEXT,
                pattern_summary TEXT,
                weight          DOUBLE,
                source          TEXT,
                added_at        TIMESTAMP,
                status          VARCHAR DEFAULT 'active'
            )
            """
        )
        added_at = datetime.now(timezone.utc).replace(tzinfo=None)
        rows = [
            (m.id, m.description, m.pattern_summary, m.weight, m.source,
             added_at, "active")
            for m in MISTAKES
        ]
        con.executemany(
            "INSERT INTO mistakes_catalog "
            "(id, description, pattern_summary, weight, source, added_at, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
    finally:
        con.close()


def export_to_md(md_path: Path = MD_PATH) -> None:
    """Write a human-readable mirror of the catalogue."""
    lines: list[str] = []
    lines.append("# Mistakes catalogue")
    lines.append("")
    lines.append(
        "Single source of truth: `catalog.py`. This file is auto-generated."
    )
    lines.append(
        "Each mistake is a (pattern, weight, source) triple. The ensemble "
        "Loss function adds `weight` for every mistake whose pattern the "
        "candidate configuration matches."
    )
    lines.append("")
    lines.append("| ID | Weight | Source | Pattern (summary) |")
    lines.append("|----|--------|--------|--------------------|")
    for m in MISTAKES:
        lines.append(
            f"| {m.id} | {m.weight:.1f} | {m.source} | "
            f"`{m.pattern_summary}` |"
        )
    lines.append("")
    lines.append("## Full descriptions")
    lines.append("")
    for m in MISTAKES:
        lines.append(f"### {m.id} (weight {m.weight:.1f})")
        lines.append("")
        lines.append(f"**Pattern:** `{m.pattern_summary}`")
        lines.append("")
        lines.append(f"**Source:** {m.source}")
        lines.append("")
        lines.append(m.description)
        lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# Self-tests — run on every bootstrap
# ---------------------------------------------------------------------------

_GOOD_CONFIG = {
    "method": "pearson_corr",
    "target_transform": "returns",
    "p_value_method": "HAC",
    "n_correction": "effective_n",
    "confirmed_decision": "deterministic_threshold",
    "subsetting_in_sql": False,
    "regime_split_check": "same_sign_required",
    "coverage_check": True,
    "out_of_sample_validation": True,
    "measured_fpr_on_shuffle": 0.05,
}

_BAD_CONFIG_V1_STYLE = {
    "method": "pearson_corr",
    "target_transform": "levels",
    "p_value_method": "classical",
    "n_correction": "raw",
    "confirmed_decision": "llm",
    "subsetting_in_sql": True,
    "regime_split_check": "any_sign",
    "coverage_check": False,
    "out_of_sample_validation": False,
    "measured_fpr_on_shuffle": None,
}


def self_test() -> None:
    """Verify matchers behave correctly on canonical good/bad configs."""
    # Good config should trigger nothing.
    total_good, triggered_good, _ = compute_penalty(_GOOD_CONFIG)
    assert total_good == 0.0, (
        f"Good config triggered mistakes {triggered_good} (total {total_good})"
    )

    # Bad config (v1-style: LLM decides). Note M002 does NOT fire here —
    # M002 is about threshold-based decisions without HAC, but this config
    # delegates to LLM (M004 territory). They are mutually exclusive by design.
    total_bad, triggered_bad, _ = compute_penalty(_BAD_CONFIG_V1_STYLE)
    expected = {"M001", "M003", "M004", "M005", "M006", "M007", "M008"}
    actual = set(triggered_bad)
    assert actual == expected, (
        f"v1-style bad config triggered {actual}, expected {expected}"
    )

    # M002 fires when decision IS threshold-based but p-value uses classical.
    threshold_no_hac = dict(_GOOD_CONFIG, p_value_method="classical")
    _, triggered_no_hac, _ = compute_penalty(threshold_no_hac)
    assert triggered_no_hac == ["M002"], (
        f"Expected only M002, got {triggered_no_hac}"
    )

    # M004 should NOT fire when threshold-based.
    assert "M004" not in triggered_no_hac, (
        "M004 wrongly fired on threshold-based decision"
    )

    # Compose: bad config + measured FPR > 0.10 should also trigger M009.
    bad_with_runtime = dict(_BAD_CONFIG_V1_STYLE, measured_fpr_on_shuffle=0.30)
    _, triggered_runtime, _ = compute_penalty(bad_with_runtime)
    assert "M009" in triggered_runtime, "M009 did not fire on measured FPR=0.30"

    # Penalty values: only-levels variant of good config = M001 alone = 1.0
    only_levels = dict(_GOOD_CONFIG, target_transform="levels")
    total_only, triggered_only, _ = compute_penalty(only_levels)
    assert triggered_only == ["M001"], f"Expected only M001, got {triggered_only}"
    assert total_only == 1.0, f"Expected 1.0, got {total_only}"

    # Penalty values: full v1-style bad config sum check
    expected_sum = sum(
        m.weight for m in MISTAKES if m.id in expected
    )
    assert abs(total_bad - expected_sum) < 1e-9, (
        f"v1 bad sum {total_bad} != expected {expected_sum}"
    )


def main() -> None:
    self_test()
    write_to_duckdb()
    export_to_md()

    total_bad, triggered_bad, breakdown = compute_penalty(_BAD_CONFIG_V1_STYLE)
    print(f"[catalog] {len(MISTAKES)} mistakes registered")
    print(f"[catalog] duckdb: {DB_PATH}")
    print(f"[catalog] md:     {MD_PATH}")
    print(f"[catalog] self-test OK")
    print(f"[catalog] v1-style config penalty = {total_bad:.1f}")
    print(f"[catalog] triggered: {triggered_bad}")


if __name__ == "__main__":
    main()
