# Methods comparison on testbed

Generated: 2026-05-14T20:02:32.898160+00:00
Methods compared: 4

## Penalty (mistakes catalogue)

| Method | Pre-penalty | Max FPR (no-signal) | Post-penalty | Triggered |
|---|---|---|---|---|
| M1_corr_levels | 4.1000000000000005 | 0.9033 | 5.1000000000000005 | M001,M002,M006,M007,M008,M009 |
| M2_corr_returns_hac | 0.9 | 0.0 | 0.9 | M008 |
| M3_granger_hac | 0.9 | 0.135 | 1.9 | M008,M009 |
| M4_event_study_hac | 0.9 | 0.065 | 0.9 | M008 |

## TPR per dataset

| dataset | M1_corr_levels | M2_corr_returns_hac | M3_granger_hac | M4_event_study_hac |
|---|---|---|---|---|
| S1 | nan | nan | nan | nan |
| S2 | nan | nan | nan | nan |
| S3 | 0.0000 | 0.0000 | 1.0000 | 1.0000 |
| S4 | 0.0000 | 1.0000 | 1.0000 | 1.0000 |
| S5 | 0.0000 | 0.0000 | 1.0000 | 0.9150 |
| S6 | nan | nan | nan | nan |

## FPR per dataset

| dataset | M1_corr_levels | M2_corr_returns_hac | M3_granger_hac | M4_event_study_hac |
|---|---|---|---|---|
| S1 | 0.0000 | 0.0000 | 0.0100 | 0.0283 |
| S2 | 0.9033 | 0.0000 | 0.0083 | 0.0283 |
| S3 | 0.0000 | 0.0000 | 0.0220 | 0.0180 |
| S4 | 0.0000 | 0.0000 | 0.0127 | 0.0227 |
| S5 | 0.0000 | 0.0000 | 0.0081 | 0.0169 |
| S6 | 0.0000 | 0.0000 | 0.1350 | 0.0650 |

## F1 per dataset

| dataset | M1_corr_levels | M2_corr_returns_hac | M3_granger_hac | M4_event_study_hac |
|---|---|---|---|---|
| S1 | nan | nan | nan | nan |
| S2 | nan | nan | nan | nan |
| S3 | — | nan | 0.9479 | 0.9569 |
| S4 | — | 1.0000 | 0.9346 | 0.8889 |
| S5 | — | nan | 0.9685 | 0.8927 |
| S6 | nan | nan | nan | nan |

## Interpretation hints

- **S1 (pure noise)**: FPR should be ~5% for any sound method.
- **S2 (spurious trend)**: FPR >> 5% signals M001 contamination.
- **S3 (r=-0.3 at lag=14)**: weak signal, may be below |r|>=0.4 gate.
- **S4 (r=-0.5 in first half)**: strong signal, expect high TPR.
- **S5 (multi-signal)**: tests selectivity (A,B real; C noise).
- **S6 (confounder)**: FPR > 5% signals lack of causal discipline.

Long-format data: `comparison.csv`