# Mistakes catalogue

Single source of truth: `catalog.py`. This file is auto-generated.
Each mistake is a (pattern, weight, source) triple. The ensemble Loss function adds `weight` for every mistake whose pattern the candidate configuration matches.

| ID | Weight | Source | Pattern (summary) |
|----|--------|--------|--------------------|
| M001 | 1.0 | DS audit 2026-05-14; Marathon 2/5; smoke v1 | `method in {corr,...} AND target_transform == 'levels'` |
| M002 | 0.8 | DS audit 2026-05-14 | `threshold-based confirm AND p_value_method != HAC/bootstrap` |
| M003 | 1.0 | smoke v1 inflation case | `regime_split_check == 'any_sign'` |
| M004 | 0.9 | Marathon 1-5; all smoke runs | `confirmed_decision == 'llm'` |
| M005 | 0.7 | smoke v1: WHERE daily_vol <= 3 | `subsetting_in_sql == True` |
| M006 | 0.6 | data_audit_v1: ruble, sanctions | `coverage_check is missing/False` |
| M007 | 0.8 | DS audit 2026-05-14 | `n_correction == 'raw'` |
| M008 | 0.9 | holdout 2026-05-02 | `out_of_sample_validation == False` |
| M009 | 1.0 | Phase A.5 measurement (TBD) | `measured_fpr_on_shuffle > 0.10` |

## Full descriptions

### M001 (weight 1.0)

**Pattern:** `method in {corr,...} AND target_transform == 'levels'`

**Source:** DS audit 2026-05-14; Marathon 2/5; smoke v1

CORR / Pearson r computed on price levels instead of returns. Two trending series will show spurious high |r| without any predictive content (Granger 1974 spurious regression).

### M002 (weight 0.8)

**Pattern:** `threshold-based confirm AND p_value_method != HAC/bootstrap`

**Source:** DS audit 2026-05-14

Confirmed by p-value, but the p-value uses the classical formula for i.i.d. samples — invalid for autocorrelated time series. Newey-West HAC or block bootstrap is required.

### M003 (weight 1.0)

**Pattern:** `regime_split_check == 'any_sign'`

**Source:** smoke v1 inflation case

Multi-regime result with opposite r signs across regimes was counted as confirmed. Real example: smoke v1 had r=-0.34 in high-inflation regime and r=+0.58 in low — confirmed=true.

### M004 (weight 0.9)

**Pattern:** `confirmed_decision == 'llm'`

**Source:** Marathon 1-5; all smoke runs

LLM emits its own confirmed=true/false and signal_score, rather than the pipeline applying a deterministic statistical gate.

### M005 (weight 0.7)

**Pattern:** `subsetting_in_sql == True`

**Source:** smoke v1: WHERE daily_vol <= 3

SQL contains arbitrary WHERE filters (volatility quantiles, ad-hoc thresholds) that were not declared in the pre-registered hypothesis. Each such filter inflates the multiple-testing burden.

### M006 (weight 0.6)

**Pattern:** `coverage_check is missing/False`

**Source:** data_audit_v1: ruble, sanctions

Topic with <30% non-null coverage in the active window admitted to analysis as a full-coverage feature. Reduces effective n in ways that classical n_min checks don't see.

### M007 (weight 0.8)

**Pattern:** `n_correction == 'raw'`

**Source:** DS audit 2026-05-14

Raw sample size n used in significance — no correction for autocorrelation. For autocorrelated returns, effective n is much smaller, and p-values are overstated.

### M008 (weight 0.9)

**Pattern:** `out_of_sample_validation == False`

**Source:** holdout 2026-05-02

No walk-forward / out-of-sample validation. sign(r) reversal between Discovery and Validation cannot be detected (historical examples: USD/RUB->MOEXFN, Brent->MOEXFN failed holdout 2026-05-02).

### M009 (weight 1.0)

**Pattern:** `measured_fpr_on_shuffle > 0.10`

**Source:** Phase A.5 measurement (TBD)

Pipeline returns >10% confirmed rate on shuffled labels. Indicates the test itself is the source of confirmations, not any real structure in data. Measured during Phase A.5.
