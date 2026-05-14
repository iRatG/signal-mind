# M3_granger_hac on testbed

Run timestamp: 2026-05-14T19:58:32.254085+00:00
Source: analytics.testbed.methods.m3_granger

## Penalty from mistakes catalogue

- **Pre-measurement penalty:** 0.9  triggers `['M008']`
- **Post-measurement penalty:** 1.9  triggers `['M008', 'M009']`
- max FPR observed (S1/S2/S6) = 0.135

## Per-dataset metrics

| Dataset | n_TP | n_FP | n_TN | n_FN | TPR | FPR | Precision | F1 |
|---|---|---|---|---|---|---|---|---|
| S1 | 0 | 6 | 594 | 0 | None | 0.01 | 0.0 | None |
| S2 | 0 | 5 | 595 | 0 | None | 0.0083 | 0.0 | None |
| S3 | 100 | 11 | 489 | 0 | 1.0 | 0.022 | 0.9009 | 0.9479 |
| S4 | 100 | 14 | 1086 | 0 | 1.0 | 0.0127 | 0.8772 | 0.9346 |
| S5 | 200 | 13 | 1587 | 0 | 1.0 | 0.0081 | 0.939 | 0.9685 |
| S6 | 0 | 81 | 519 | 0 | None | 0.135 | 0.0 | None |

## Diagnostic notes

- High FPR on S1: method gives false positives on pure noise.
- High FPR on S2: method falls into the spurious-trend trap (M001).
- TPR on S3 measures statistical power at the true lag.
- High FPR on S6: method confuses confounded correlation with causation.
- TPR=0 on S3/S4/S5 means the method is blind to genuine signals.

Raw verdicts: `M3_granger_hac_raw.csv` (one row per seed × hypothesis)
Summary CSV : `M3_granger_hac_summary.csv`