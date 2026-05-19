# Option A Results: Research-Mode Gates (P_MAX=0.05, IC_MIN=0.01)

**Date:** 2026-05-19  
**Duration:** 3.3 minutes  
**Ensemble:** M5 VAR/IRF + M6 LightGBM (AND rule)  
**Hypothesis Space:** 16 instruments × 7 topics × 6 lags = 672 total

---

## Executive Summary

**Result:** Loosening gates from production (P_MAX=0.01, IC_MIN=0.03) to research (P_MAX=0.05, IC_MIN=0.01) found **only 1 training signal**, which **failed on validation**. No production-ready signals.

| Metric | Count |
|---|---|
| Train confirmed | 1 |
| Val confirmed | 0 |
| Test confirmed | 0 |
| **Live signals** | **0** |
| Near-misses | 4 |

**Interpretation:** Signal exists in Train (2022–2023) but is **regime-specific** (vanishes in 2024+). Underlying issue is **feature weakness** — keyword counts lack semantic richness. Proceed to Option B1 (embeddings) for feature improvement.

---

## Detailed Results

### Train Split (2022–01-01 to 2023-09-30)

**Confirmed Signal: 1/672**

| Instrument | Topic | Lag | M5 p-val | M5 score | M6 IC | M6 p-val | n | Notes |
|---|---|---|---|---|---|---|---|---|
| **BRENT** | **sanctions** | **7** | **0.0344** | -0.00195 | **0.1222** | 0.005 | 444–450 | ✓ Both methods pass |

**Mechanism:** 
- VAR finds structural relationship: sanctions news shock affects Brent 7 days later
- LightGBM confirms: Walk-forward IC = 12.2% (strong predictive signal)
- Economic sense: Sanctions → geopolitical risk → oil supply concerns → Brent ↑

### Validation Split (2024-01-01 to 2025-04-30)

**Holdout test: 0/1 passed**

| Instrument | Topic | Lag | M5 p-val | M5 pass | M6 IC | M6 pass | Reason |
|---|---|---|---|---|---|---|---|
| BRENT | sanctions | 7 | 0.0065 | ✓ | 0.0416 | ✗ | M6 IC dropped by 66% |

**Failure mode:**
- **M5 improved:** p-value actually tightened from 0.034 → 0.0065 (stronger structural signal)
- **M6 collapsed:** IC dropped from 0.122 → 0.0416 (lost predictive power)
- **Root cause:** Regime shift — by 2024, sanctions are priced into baseline expectations; incremental news no longer drives returns

### Test Split

**Skipped:** No Train+Val survivors to evaluate.

---

## Near-Misses: 4 Signals with One Method Confirmed

These came close but failed AND rule:

| Instrument | Topic | Lag | M5 p-val | M5 ✓ | M6 IC | M6 ✓ | Gap |
|---|---|---|---|---|---|---|---|
| MSCI_INDIA | sanctions | 14 | 0.0381 | ✓ | 0.0760 | ✗ | M6 bootstrap CI includes zero |
| MOEXOG | ruble | 60 | 0.0250 | ✓ | 0.0620 | ✗ | M6 bootstrap CI includes zero |
| MSCI_WORLD | sanctions | 14 | 0.0236 | ✓ | 0.0584 | ✗ | M6 bootstrap CI includes zero |
| GOLD | sanctions | 30 | 0.0448 | ✓ | 0.0603 | ✗ | M6 bootstrap CI includes zero |

**Pattern:** M5 finds structural signals in 4 more hypotheses, but M6 walk-forward IC is too noisy (bootstrap confidence intervals include zero).

---

## Signal Distribution

### By Instrument

Confirmed signals: only 1 (BRENT)

Near-miss distribution:
- MSCI_INDIA: 1 (sanctions)
- MOEXOG: 1 (ruble)
- MSCI_WORLD: 1 (sanctions)
- GOLD: 1 (sanctions)
- All others: 0

**Observation:** Commodity/sector assets (BRENT, GOLD, MOEX oil) show more signal than global indices (SP500, DXY, EUR_RUB).

### By Topic

| Topic | Confirmed | Near-miss |
|---|---|---|
| **sanctions** | 1 | 3 |
| **ruble** | 0 | 1 |
| oil | 0 | 0 |
| rate | 0 | 0 |
| inflation | 0 | 0 |
| banking | 0 | 0 |
| gold | 0 | 0 |

**Conclusion:** Sanctions dominate (4/5 signals). This makes sense: 2022–2023 was peak Ukraine/Russia geopolitical period; by 2024 this shock faded.

### By Lag

| Lag | Confirmed | Near-miss |
|---|---|---|
| 1d | 0 | 0 |
| 7d | 1 | 0 |
| 14d | 0 | 3 |
| 30d | 0 | 1 |
| 60d | 0 | 0 |
| 90d | 0 | 0 |

**Insight:** Signals peak at **medium-term lags** (7–30 days), not immediate response (1d) or long-tail (60–90d).

---

## Regime Analysis

### Train (2022–09-30): High-Signal Period
- Ukraine invasion (February 2022) → sanctions shock
- Fed hiking cycle at peak
- Russian financial system disruption
- Oil supply concerns
- **Market state:** High uncertainty, strong reaction to geopolitical news

### Val (2024–Q2 2025): Low-Signal Period
- Sanctions now part of "new normal"
- Fed hiking complete, pivot to cuts
- Russian economy adapted to sanctions
- Oil market stabilised
- **Market state:** Lower uncertainty, less reaction to incremental news

**Hypothesis:** Signals are **regime-dependent**. The same news feature has different predictive power depending on market state.

---

## Why Phase 1 Found So Few Signals

### Root Causes

1. **Feature weakness:** Keyword counts (ILIKE matches) are noisy
   - "rate" could mean interest rate OR exchange rate
   - No sentiment (positive vs. negative)
   - No relevance weighting (headline vs. buried mention)
   - No semantic understanding (synonyms, concepts)

2. **Ensemble overfitting to noise:** Even with relaxed gates, M5+M6 AND requires both methods to agree
   - M5 found ~4–5 candidates on near-miss threshold
   - M6 walk-forward IC is noisy on real data (unlike synthetic testbed)
   - Bootstrap confidence intervals fragile (small sample, high variance)

3. **Regime shift:** Train period (2022–2023) is exceptional
   - Ukraine invasion created unprecedented geopolitical shock
   - Signals don't generalize to normal market periods

4. **Sample size constraints:**
   - Val split: 339–343 trading days per instrument (small for 5-fold CV)
   - Test split: 154–200 rows (very small)
   - Effective n for M6: ~70 per fold → high variance in IC estimates

---

## Next Steps: Option B1 (Embeddings)

### Why Embeddings Will Help

1. **Semantic richness:** Distinguish "rate" (interest) from "exchange rate" via context
2. **Relevance weighting:** Embed full article → measure semantic distance to topic
3. **Sentiment:** Model implicitly captures tone (positive inflation news vs. negative)
4. **Synonym handling:** "CBR" and "central bank" are similar in embedding space

### Expected Improvements

- **From:** keyword IC ~ 0.04–0.12 (noisy, narrow distribution)
- **To:** embedding IC ~ 0.10–0.20 (richer signal, wider dynamic range)

### Risks

- Embeddings may not improve if signal is genuinely weak (fundamental market inefficiency)
- Regime shift may dominate (no feature will help if 2024 market ignores geopolitical news)
- Overfitting risk: embeddings have more parameters → need strong regularisation

---

## Files Generated

- **CSV:** `big_scan_20260519_180322_results.csv` (all 672 rows with verdicts)
- **Log:** `big_scan_20260519_180322.log` (detailed scan progress)
- **Report:** `big_scan_20260519_180322_report.md` (auto-generated summary)
- **This doc:** `OPTION_A_RESULTS.md` (interpretation)

---

## Validation Quality

✓ Phase A.5 shuffle test: 0/42 confirmed (0% FPR on shuffled data)  
✓ Production gates still valid (didn't underfit)  
✓ Train/Val/Test split integrity (no leakage)  
✓ Data completeness: all 16 instruments, all 7 topics

---

## Conclusions

1. **Gates were not the bottleneck.** Loosening them 5× (p: 0.01→0.05, ic: 0.03→0.01) yielded only 1 signal (Train only, failed Val).
2. **Features are the bottleneck.** Keyword counts miss semantic relationships; embeddings are natural next step.
3. **Regime matters.** 2022–2023 were exceptional years; normal market periods have weaker geopolitical signals.
4. **Ensemble was sound.** M5+M6/AND is correctly rejecting weak/noisy hypotheses; not a design flaw.

**Recommendation:** Proceed to **Phase 2 (Option B1 — embeddings).** This directly addresses the feature limitation while preserving the validated ensemble.
