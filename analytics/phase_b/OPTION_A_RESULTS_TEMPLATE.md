# Option A Results: Research-Mode Gates (P_MAX=0.05, IC_MIN=0.01)

**Date:** 2026-05-19  
**Gates:** P_MAX=0.05 (was 0.01), IC_MIN=0.01 (was 0.03)  
**Ensemble:** M5 + M6 AND  
**Total hypotheses:** 672 (16 instruments × 7 topics × 6 lags)

---

## Summary

| Split | Hypotheses | Confirmed | Rate |
|---|---|---|---|
| Train | 672 | ? | ? |
| Val (filtered) | ? | ? | ? |
| Test (filtered) | ? | ? | ? |
| **Total** | - | **?** | **?** |

---

## Confirmed Signals by Instrument

| Instrument | Count | Top Signal (topic, lag, IC) |
|---|---|---|
| SP500 | ? | |
| USD_RUB | ? | |
| EUR_RUB | ? | |
| BRENT | ? | (sanctions, 7, 0.12) |
| ... | | |
| **Total** | **?** | |

---

## Confirmed Signals by Topic

| Topic | Count | Stability |
|---|---|---|
| oil | ? | |
| rate | ? | |
| ruble | ? | |
| sanctions | ? | |
| inflation | ? | |
| banking | ? | |
| gold | ? | |
| **Total** | **?** | |

---

## Confirmed Signals by Lag

| Lag | Count |
|---|---|
| 1 day | ? |
| 7 days | ? |
| 14 days | ? |
| 30 days | ? |
| 60 days | ? |
| 90 days | ? |
| **Total** | **?** |

---

## Top 15 Strongest Signals

| Instrument | Topic | Lag | M5 p-val | M6 IC | Status |
|---|---|---|---|---|---|
| BRENT | sanctions | 7 | 0.0344 | 0.1222 | Train ✓ Val ✓ Test ? |
| | | | | | |

---

## Regime Analysis

**Question:** Do signals change across Train (2022–2023) vs. Val (2024–Q2 2025)?

- Signal consistency (same instrument/topic across splits): ?
- Disappearing signals (Train ✓ but Val ✗): ?
- Emerging signals (Train ✗ but Val ✓): ?

---

## Comparison to Baseline (Production Gates)

Previous run (P_MAX=0.01, IC_MIN=0.03) found: **0 signals**  
This run (P_MAX=0.05, IC_MIN=0.01) found: **? signals**

Improvement factor: **?**

---

## Insights & Patterns

### Which instruments have the most signal?
- MOEX indices (imoex, moexfn, moexog, moex10) vs. global indices (SP500, MSCI_WORLD)?
- Forex (USD_RUB, EUR_RUB) vs. commodities (BRENT, GOLD, SILVER)?

### Which topics drive signals?
- Sanctions/geopolitical (strongest for BRENT, MOEX)?
- Monetary policy (rate) for forex?
- Inflation for metals?

### What lag is most common?
- Short-term (1–7 days): quick market response
- Medium-term (14–30 days): slower repricing
- Long-term (60–90 days): regime shifts?

### Geographic pattern?
- Russian assets (MOEX, RUB) driven by Russian news (sanctions, rate, ruble)?
- Global assets (SP500, MSCI_WORLD) driven by global news (oil, inflation)?

---

## Next Steps

1. **If signals are strong + interpretable:** Proceed to Phase B (real train run) or Option B1 (embeddings)
2. **If signals are weak/scattered:** Consider Option C (regime detection) or re-examine data quality
3. **If signals cluster in one instrument:** Investigate specialisation (e.g., MOEX-only bundle)

---

## Files Generated

- `big_scan_20260519_*.csv` — all 672 hypotheses with verdicts
- `big_scan_20260519_*.log` — detailed run log
- This report

---

## Data Quality Checks

- Phase A.5 shuffle test: 0/42 confirmed ✓ (gates not too loose)
- Test split minimum size (BRENT): 170 rows
- Feature coverage: all 7 topics in news_daily
- Market data: all 16 instruments loaded

---

## Caveats

- Research gates (p<0.05) are relaxed for **discovery only** — not for production trading
- Signals may not generalize to 2024+ data (Val/Test performance TBD)
- Phase A.5 uses synthetic data; real data has different characteristics
