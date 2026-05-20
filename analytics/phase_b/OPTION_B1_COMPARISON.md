# Option B1 Comparison: Keyword vs Embedding Features

**Date:** 2026-05-20  
**Hypotheses:** 672 (16 instruments × 7 topics × 6 lags)  
**Ensemble:** M5+M6/AND, research gates (P_MAX=0.05, IC_MIN=0.01)  
**Embedding model:** paraphrase-multilingual-MiniLM-L12-v2  
**Embedding period:** 2022-01-01 → 2025-04-30 (1210 days, 14 × 90-day chunks)

---

## Summary

| Metric | Keyword | Embedding | Δ |
|---|---|---|---|
| Train confirmed | **1** | **0** | -1 |
| Val confirmed | 0 | 0 | — |
| Test confirmed | 0 | 0 | — |
| Near-misses (M5✓, M6✗) | 8 | **14** | +6 |
| M6 IC mean | 0.0419 | 0.0429 | +2.4% |
| M6 IC max | 0.1726 | **0.1779** | +3.1% |
| M5 p-val min | 0.0160 | **0.0076** | better |
| IC ≥ 0.05 hypotheses | 217 | **245** | +13% |
| IC ≥ 0.10 hypotheses | 43 | 43 | 0% |

**Conclusion: Embeddings marginally improve feature quality but do not unlock new production-grade signals.**

---

## Detailed Analysis

### What Embeddings Improved

1. **M6 IC distribution shifted slightly upward** (+2.4% mean, +13% at IC≥0.05 threshold)  
   Semantics reduce noise for medium-strength signals

2. **More near-misses** (14 vs 8): M5 structurally confirms more hypotheses with embeddings  
   Particularly MOEXOG sector with banking/sanctions/rate topics

3. **Stronger M5 signals where they exist** (min p-val: 0.0076 vs 0.0160)  
   Embedding features carry stronger structural relationship in VAR

### What Embeddings Did NOT Improve

1. **No new AND-confirmed signals** (0 vs 1 with keyword)  
   The marginal IC gain (+2.4%) is insufficient to cross both thresholds simultaneously

2. **Same IC ceiling at high thresholds** (IC≥0.10: both = 43 hypotheses)  
   Top of the distribution unchanged — ceiling is market signal strength, not feature quality

3. **M5/M6 disagreement persists**: Where M6 has high IC, M5 p-val is poor — and vice versa

### Top Embedding Signals (Near-Misses)

| Instrument | Topic | Lag | M5 p-val | M6 IC | M5✓ | M6✓ | Bottleneck |
|---|---|---|---|---|---|---|---|
| USD_RUB | gold_emb | 90 | 0.149 | **0.178** | ✗ | ✗ | M5 weak |
| EUR_RUB | gold_emb | 90 | 0.149 | **0.178** | ✗ | ✗ | M5 weak |
| FTSE_CHINA_50 | oil_emb | 90 | 0.941 | **0.144** | ✗ | ✓ | M5 very weak |
| MOEXOG | banking_emb | 90 | 0.536 | 0.132 | ✗ | ✓ | M5 weak |
| MOEXOG | sanctions_emb | 1 | 0.352 | 0.126 | ✗ | ✓ | M5 weak |
| MOEXOG | rate_emb | 30 | 0.472 | 0.123 | ✗ | ✓ | M5 weak |

**Pattern:** MOEXOG (Russian oil sector) consistently shows M6-confirmed signals across banking, sanctions, rate topics. But M5 (VAR with confounder control) fails to find structural causality.

---

## Root Cause Diagnosis

### Why Neither Feature Type Finds Signals

The problem is not features — it is **M5/M6 structural disagreement** on real financial data:

```
M5 (VAR/IRF):   Finds causal structure   → confirms few hypotheses (strict p<0.05)
M6 (LightGBM):  Finds predictive pattern → confirms different hypotheses (IC>0.01)
AND rule:        Requires BOTH → almost nothing passes
```

**M5 and M6 capture fundamentally different things:**
- M5 VAR: "Does news Granger-cause market returns, accounting for confounders?"
- M6 LGBM: "Can news predict return direction out-of-sample?"

These can differ when:
- Signal is predictive but non-causal (correlation with omitted variable)
- Signal is structurally causal but noisy / regime-specific (M5 passes, M6 noisy)
- Sample is small (Val/Test splits are 170-350 rows → M6 high variance)

### What This Means

The AND ensemble was correctly designed for **testbed synthetic data** where:
- Signal-to-noise is controlled
- Both methods should find the same signal

But on **real financial data**, market signals are:
- Regime-specific (strong in 2022, faded in 2024)
- Structurally causal AND predictively reliable at different times
- Affected by confounders M5 controls for (which M6 ignores)

---

## What We Know Now

| Question | Answer |
|---|---|
| Are signals genuinely present? | **Yes** — BRENT/sanctions (Train), MOEXOG cluster, rolling scanner found 47 in windowed analysis |
| Are keyword features the bottleneck? | **No** — embeddings gave same IC ceiling |
| Is feature engineering the bottleneck? | **Partially** — IC improved +2.4% but not enough |
| Is the AND ensemble too strict for real data? | **Probably** — M5 and M6 don't agree on same hypotheses |
| Is regime instability a factor? | **Yes** — signals present in 2022–2023, absent in 2024+ |

---

## Next Decision Point

Given what we know, the honest options are:

### Option C: Regime Detection + Conditional Ensemble
- Segment Train into 2022 (crisis) vs 2023 (normalisation) regimes
- Test M5/M6 within-regime only
- Hypothesis: BRENT/sanctions confirmed in 2022 window but not 2023
- Effort: 1-2 days

### Option F: M6-Only with Strict Bootstrap
- Remove M5 from ensemble (it finds different type of signal)
- Use M6 walk-forward IC ≥ 0.05 + bootstrap 5th-pct > 0.01 (not > 0)
- Accept that signals may not be structurally causal (predictive only)
- Risk: higher FPR → need larger bootstrap sample or stronger IC gate
- Effort: 1 day

### Option G: Honest Acceptance
- Signals in Russian financial market are genuinely weak and regime-specific
- Rolling scanner provides the right framing: signals exist episodically (~17% of windows)
- Accept: no persistent production signal exists with current data/methods
- Focus on finding the strongest episodic signals for further investigation

---

## Recommendation

**Proceed to Option C (Regime Detection)** as the next methodical step:

1. The rolling scanner already showed episodic signals — regime is the pattern
2. This is the natural extension of what we already know
3. Preserves ensemble integrity (we learn WHEN the signal exists, not relax standards)
4. One more honest test before accepting Option G

If regime detection still yields 0 confirmed signals → accept Option G (honest outcome).

---

## Validation Summary

| Test | Result |
|---|---|
| Phase A.5 keyword shuffle | **0/42 = 0% FPR** ✓ |
| Phase A.5 embedding shuffle | **0/42 = 0% FPR** ✓ |
| Embedding mean IC vs keyword | +2.4% (marginal, not decisive) |
| Embedding max IC | 0.178 vs 0.173 (+3%) |
| AND-confirmed signals | Embedding: 0, Keyword: 1 (Train only) |

Both feature types are well-calibrated. Problem is market signal strength, not methodology.
