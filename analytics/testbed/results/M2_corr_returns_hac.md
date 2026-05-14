# M2_corr_returns_hac on testbed

Run timestamp: 2026-05-14T19:49:41.853589+00:00
Source: analytics.testbed.methods.m2_corr_returns

## Penalty from mistakes catalogue

- **Pre-measurement penalty:** 0.9  triggers `['M008']`
- **Post-measurement penalty:** 0.9  triggers `['M008']`
- max FPR observed (S1/S2/S6) = 0.0

## Per-dataset metrics

| Dataset | n_TP | n_FP | n_TN | n_FN | TPR | FPR | Precision | F1 |
|---|---|---|---|---|---|---|---|---|
| S1 | 0 | 0 | 600 | 0 | None | 0.0 | None | None |
| S2 | 0 | 0 | 600 | 0 | None | 0.0 | None | None |
| S3 | 0 | 0 | 500 | 100 | 0.0 | 0.0 | None | None |
| S4 | 100 | 0 | 1100 | 0 | 1.0 | 0.0 | 1.0 | 1.0 |
| S5 | 0 | 0 | 1600 | 200 | 0.0 | 0.0 | None | None |
| S6 | 0 | 0 | 600 | 0 | None | 0.0 | None | None |

## Diagnostic notes

- High FPR on S1: method gives false positives on pure noise.
- High FPR on S2: method falls into the spurious-trend trap (M001).
- TPR on S3 measures statistical power at the true lag.
- High FPR on S6: method confuses confounded correlation with causation.
- TPR=0 on S3/S4/S5 means the method is blind to genuine signals.

Raw verdicts: `M2_corr_returns_hac_raw.csv` (one row per seed × hypothesis)
Summary CSV : `M2_corr_returns_hac_summary.csv`