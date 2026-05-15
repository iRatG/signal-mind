# M6_lgbm_walkforward on testbed

Run timestamp: 2026-05-14T21:01:02.817572+00:00
Source: analytics.testbed.methods.m6_lgbm

## Penalty from mistakes catalogue

- **Pre-measurement penalty:** 0.0  triggers `[]`
- **Post-measurement penalty:** 1.0  triggers `['M009']`
- max FPR observed (S1/S2/S6) = 0.1183

## Per-dataset metrics

| Dataset | n_TP | n_FP | n_TN | n_FN | TPR | FPR | Precision | F1 |
|---|---|---|---|---|---|---|---|---|
| S1 | 0 | 21 | 579 | 0 | None | 0.035 | 0.0 | None |
| S2 | 0 | 20 | 580 | 0 | None | 0.0333 | 0.0 | None |
| S3 | 100 | 24 | 476 | 0 | 1.0 | 0.048 | 0.8065 | 0.8929 |
| S4 | 100 | 34 | 1066 | 0 | 1.0 | 0.0309 | 0.7463 | 0.8547 |
| S5 | 198 | 61 | 1539 | 2 | 0.99 | 0.0381 | 0.7645 | 0.8627 |
| S6 | 0 | 71 | 529 | 0 | None | 0.1183 | 0.0 | None |

## Diagnostic notes

- High FPR on S1: method gives false positives on pure noise.
- High FPR on S2: method falls into the spurious-trend trap (M001).
- TPR on S3 measures statistical power at the true lag.
- High FPR on S6: method confuses confounded correlation with causation.
- TPR=0 on S3/S4/S5 means the method is blind to genuine signals.

Raw verdicts: `M6_lgbm_walkforward_raw.csv` (one row per seed × hypothesis)
Summary CSV : `M6_lgbm_walkforward_summary.csv`