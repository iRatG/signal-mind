# M4_event_study_hac on testbed

Run timestamp: 2026-05-14T20:02:11.737697+00:00
Source: analytics.testbed.methods.m4_event_study

## Penalty from mistakes catalogue

- **Pre-measurement penalty:** 0.9  triggers `['M008']`
- **Post-measurement penalty:** 0.9  triggers `['M008']`
- max FPR observed (S1/S2/S6) = 0.065

## Per-dataset metrics

| Dataset | n_TP | n_FP | n_TN | n_FN | TPR | FPR | Precision | F1 |
|---|---|---|---|---|---|---|---|---|
| S1 | 0 | 17 | 583 | 0 | None | 0.0283 | 0.0 | None |
| S2 | 0 | 17 | 583 | 0 | None | 0.0283 | 0.0 | None |
| S3 | 100 | 9 | 491 | 0 | 1.0 | 0.018 | 0.9174 | 0.9569 |
| S4 | 100 | 25 | 1075 | 0 | 1.0 | 0.0227 | 0.8 | 0.8889 |
| S5 | 183 | 27 | 1573 | 17 | 0.915 | 0.0169 | 0.8714 | 0.8927 |
| S6 | 0 | 39 | 561 | 0 | None | 0.065 | 0.0 | None |

## Diagnostic notes

- High FPR on S1: method gives false positives on pure noise.
- High FPR on S2: method falls into the spurious-trend trap (M001).
- TPR on S3 measures statistical power at the true lag.
- High FPR on S6: method confuses confounded correlation with causation.
- TPR=0 on S3/S4/S5 means the method is blind to genuine signals.

Raw verdicts: `M4_event_study_hac_raw.csv` (one row per seed × hypothesis)
Summary CSV : `M4_event_study_hac_summary.csv`