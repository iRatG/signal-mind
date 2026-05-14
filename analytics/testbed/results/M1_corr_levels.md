# M1_corr_levels on testbed

Run timestamp: 2026-05-14T19:49:27.347867+00:00
Source: analytics.testbed.methods.m1_corr_levels

## Penalty from mistakes catalogue

- **Pre-measurement penalty:** 4.1  triggers `['M001', 'M002', 'M006', 'M007', 'M008']`
- **Post-measurement penalty:** 5.1  triggers `['M001', 'M002', 'M006', 'M007', 'M008', 'M009']`
- max FPR observed (S1/S2/S6) = 0.9033

## Per-dataset metrics

| Dataset | n_TP | n_FP | n_TN | n_FN | TPR | FPR | Precision | F1 |
|---|---|---|---|---|---|---|---|---|
| S1 | 0 | 0 | 600 | 0 | None | 0.0 | None | None |
| S2 | 0 | 542 | 58 | 0 | None | 0.9033 | 0.0 | None |
| S3 | 0 | 0 | 500 | 100 | 0.0 | 0.0 | None | None |
| S4 | 0 | 0 | 1100 | 100 | 0.0 | 0.0 | None | None |
| S5 | 0 | 0 | 1600 | 200 | 0.0 | 0.0 | None | None |
| S6 | 0 | 0 | 600 | 0 | None | 0.0 | None | None |

## Diagnostic notes

- High FPR on S1: method gives false positives on pure noise.
- High FPR on S2: method falls into the spurious-trend trap (M001).
- TPR on S3 measures statistical power at the true lag.
- High FPR on S6: method confuses confounded correlation with causation.
- TPR=0 on S3/S4/S5 means the method is blind to genuine signals.

Raw verdicts: `M1_corr_levels_raw.csv` (one row per seed × hypothesis)
Summary CSV : `M1_corr_levels_summary.csv`