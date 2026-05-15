# M5_var_orth_irf on testbed

Run timestamp: 2026-05-14T20:42:11.931333+00:00
Source: analytics.testbed.methods.m5_var

## Penalty from mistakes catalogue

- **Pre-measurement penalty:** 0.9  triggers `['M008']`
- **Post-measurement penalty:** 0.9  triggers `['M008']`
- max FPR observed (S1/S2/S6) = 0.0067

## Per-dataset metrics

| Dataset | n_TP | n_FP | n_TN | n_FN | TPR | FPR | Precision | F1 |
|---|---|---|---|---|---|---|---|---|
| S1 | 0 | 4 | 596 | 0 | None | 0.0067 | 0.0 | None |
| S2 | 0 | 1 | 599 | 0 | None | 0.0017 | 0.0 | None |
| S3 | 100 | 5 | 495 | 0 | 1.0 | 0.01 | 0.9524 | 0.9756 |
| S4 | 100 | 2 | 1098 | 0 | 1.0 | 0.0018 | 0.9804 | 0.9901 |
| S5 | 200 | 7 | 1593 | 0 | 1.0 | 0.0044 | 0.9662 | 0.9828 |
| S6 | 0 | 1 | 599 | 0 | None | 0.0017 | 0.0 | None |

## Diagnostic notes

- High FPR on S1: method gives false positives on pure noise.
- High FPR on S2: method falls into the spurious-trend trap (M001).
- TPR on S3 measures statistical power at the true lag.
- High FPR on S6: method confuses confounded correlation with causation.
- TPR=0 on S3/S4/S5 means the method is blind to genuine signals.

Raw verdicts: `M5_var_orth_irf_raw.csv` (one row per seed × hypothesis)
Summary CSV : `M5_var_orth_irf_summary.csv`