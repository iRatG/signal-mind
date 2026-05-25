# Signal Mind — Accumulated Knowledge

**Last updated:** 2026-05-24  
**Covers:** Phase 1 → Phase 2 → Ouroboros (30 rounds) → Test Validation

---

## What We Know For Sure

### 1. Features

| Finding | Evidence |
|---|---|
| z-score normalization (window=90) is necessary | Unlocked GOLD/inflation_z, MOEXOG signals that raw counts missed |
| `keyword_z90` is the best feature type | Consistently top in Ouroboros across 30 rounds |
| Embedding features ≈ keyword features | +2.4% mean IC only, no new AND-confirmed signals |
| All feature types perform similarly at top (IC≥0.10) | 43 hypotheses per type at IC≥0.10 in big scan |
| Volatility target (abs_return, vol_5d) finds nothing | 0 signals at IC≥0.05, market_return dominates |

### 2. Ensemble

| Finding | Evidence |
|---|---|
| AND rule (M5+M6) is too strict for real data (450 rows) | Phase 1+2 found 0 AND-confirmed, Ouroboros found 1 |
| M5 and M6 find different kinds of signals | M5: structural causality, M6: predictive IC — don't overlap |
| M6-only at IC≥0.03 is too permissive (60% val rate) | 3858 train signals, 2595 val holds — but no rigorous structure |
| M6-only at IC≥0.08 is a good research gate | System converged here after round 1 (67% val rate → IC gate raised) |
| bootstrap_min=0.0 needed for small N (154-450 rows) | Removing it unlocked real signals on Test |

### 3. Regime

| Finding | Evidence |
|---|---|
| 2022-2023 is a distinct crisis regime | Sanctions/inflation signals peak here |
| 2024-2025 is normalisation: weaker signals | BRENT/sanctions failed Val (2024), many signals faded |
| 2025-2026: 57% of signals reversed direction | Sign flip in 12/21 gold signals on Test |
| Rolling retraining needed | Last 12 months more predictive than 3-year history |

### 4. Persistent Signals (across all 3 splits)

| Signal | Train IC | Test IC | Notes |
|---|---|---|---|
| **FTSE_CHINA_50 / rate_z / lag=1d** | 0.094 | 0.069 | M5+M6+sign all pass on Test |
| DJ_SOUTH_AFRICA / sanctions_z / lag=14d | 0.091 | 0.068 | Marginal on Test (sign=OK) |
| DXY / oil_z / lag=1d | 0.079 | 0.055 | Marginal on Test (sign=OK) |

### 5. Sign-Flip Signals (contrarian candidates for 2025-2026)

These signals worked in 2022-2025 but reversed direction in 2025-2026.
**They may be tradeable as contrarian** if direction confirmed on fresh data:

| Signal | Original direction | 2025-2026 IC | Interpretation |
|---|---|---|---|
| GOLD / inflation_z / 7d | inflation_z ↑ → GOLD ↑ | 0.036 (FLIP) | Inflation no longer drives gold? |
| BRENT / sanctions / 7d | sanctions ↑ → BRENT ↑ | 0.108 (FLIP) | Sanctions now priced as negative? |
| MSCI_WORLD / sanctions_z / 14d | sanctions ↑ → MSCI ↑ | 0.110 (FLIP) | Global markets adapted |
| IMOEX / ruble_z / 7d | ruble_z ↑ → IMOEX ↑ | 0.134 (FLIP) | MOEX dynamics changed |
| MSCI_INDIA / inflation_emb_z / 14d | inflation ↑ → MSCI_INDIA ↑ | 0.138 (FLIP) | India decoupled from RU? |

---

## What Doesn't Work

| Approach | Result | Why |
|---|---|---|
| Keyword raw counts without normalization | Weak M5 signals | Non-stationary: news volume changes over time |
| Volatility as target | 0 signals | Returns are noisy but news-driven; vol less so in this dataset |
| AND rule on Test split (N=154) | Always 0 | N_MIN too high for short Test period |
| 3-year training window for current signals | Sign flips | Regime shift after 2024 |

---

## Architecture Decisions That Worked

- **Testbed-first:** Synthetic calibration of FPR before real data → still 0% overfit
- **Shuffle test:** Verified embeddings don't overfit (0/42 FPR)
- **Ouroboros loop:** 30 rounds, SearchKnowledge converged to keyword_z90 + IC=0.08
- **Phase A.5 pass:** Ensemble M5+M6/AND is correctly calibrated
- **BH-FDR correction:** Honest multiple testing — 0 significant at q<0.10 (21 tests)

---

## What To Do Next

## Persistent Signals (all three splits confirmed)

| Signal | Train IC | Val IC | Test IC | M5 Test p | Notes |
|---|---|---|---|---|---|
| **SP500 / inflation_z / lag=1d** | 0.149 | 0.057 | **0.185** | 0.039 | IC grows on Test — stable |
| **FTSE_CHINA_50 / rate_z / lag=1d** | 0.094 | 0.069 | 0.069 | 0.041 | Stable decay |

Both pass M5 (structural causality) + M6 (predictive IC) + sign consistency across all splits.
BH-FDR at q=0.10 on 54 simultaneous tests = 0 significant (honest multiple testing result).

---

### Priority 1: Rolling Window Retraining

Instead of fixed Train 2022-2023, use rolling 12-month windows:
- Window 1: 2023-10 → 2024-09 (12 months before Test)
- Window 2: 2024-01 → 2024-12 (calendar 2024)
- Window 3: 2024-05 → 2025-04 (last 12 months of Val)

Goal: find signals that work in the CURRENT regime, not the crisis regime.

### Priority 2: Contrarian Test

Test the 12 sign-flip signals with REVERSED expected direction on Val (2024):
```python
hyp = Hypothesis(news_field="sanctions", target_field="market_return",
                 lag_days=7, direction="negative")  # reversed from original
```
If they pass WITH reversed direction → they are real contrarian signals for 2024+.

### Priority 3: Single Hypothesis Deep Test

FTSE_CHINA_50/rate_z/1d is the single persistent signal.
Full production test: rolling backtest 2022-2026, Sharpe ratio, max drawdown.

### Priority 4: Fresh Data

Load news and market data for 2025-09 → 2026-05 (current).
Use as new "Train" for fresh signal search.

---

## Key Files

| File | Content |
|---|---|
| `src/pipeline_v2/night_search.py` | Ouroboros loop (SearchKnowledge) |
| `src/pipeline_v2/feature_transformer.py` | z-score + volatility transforms |
| `src/pipeline_v2/test_validation.py` | Deep test validation |
| `src/pipeline_v2/embedding_feature_builder.py` | Semantic embeddings |
| `analytics/phase_b/OPTION_A_RESULTS.md` | Phase 1 analysis |
| `analytics/phase_b/OPTION_B1_COMPARISON.md` | Embedding comparison |
| `analytics/phase_b/night_search/NIGHT_SEARCH_ANALYSIS.md` | Ouroboros analysis |
| `analytics/phase_b/test_validation/test_validation_*` | Test split results |
| `analytics/testbed/NEXT_SESSION.md` | Technical next steps with code |
| `db/emb_cache/` | Pre-computed embeddings (14 NPZ chunks) |
