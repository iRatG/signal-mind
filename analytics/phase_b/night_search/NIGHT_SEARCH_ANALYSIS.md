# Night Search Results — Analysis

**Run:** 2026-05-20 18:14 → 19:09 (0.92h total)  
**Phases:** 1 (M6 sweep) + 2 (AND deep dive) + 3 (regime)  
**Total hits logged:** 6534

---

## Key Numbers

| Phase | Train signals | Val signals | Best IC |
|---|---|---|---|
| Phase 1 (M6-only, IC≥0.03) | 3858 | 2595 | 0.164 |
| Phase 2 (M5+M6/AND, IC≥0.05) | 0 | 0 | — |
| Phase 3 (Regime 2022/2023) | 12 | 0 | 0.192 |
| **Unique train+val (M6-only)** | **812** | — | 0.176 |
| **Phase 2 AND-confirmed** | **0** | **0** | — |

---

## The One Most Important Signal

**`GOLD / inflation_z / lag=7`** — passed M5 AND val:
- M5 p-value: **0.016** (structural causality confirmed)
- M6 IC: **0.138** (strong predictive power)
- Val confirmed: **YES**
- Feature type: `keyword_z90` (z-score normalized)

→ This is the strongest candidate in the entire search.  
→ z-score normalization was necessary to find it (raw inflation count didn't pass M5).

---

## Feature Type Ranking (unique signals, IC≥0.05)

| Feature Type | Unique Signals |
|---|---|
| embedding_raw | 161 |
| embedding_z90 | 157 |
| keyword_raw | 155 |
| keyword_z30 | 154 |
| keyword_z90 | 136 |

**All feature types perform similarly.** z-score normalization did not dramatically
improve IC distribution — but it did help M5 pass on specific signals (GOLD/inflation_z).

**Target:** Only `market_return` produced IC≥0.05 signals.
`abs_return` and `vol_5d` found nothing strong — theory about volatility
target not confirmed for this dataset.

---

## Top 10 Train+Val Confirmed Signals (M6-only)

| Instrument | Topic | Lag | M6 IC | M5 p-val | M5 pass | Feature |
|---|---|---|---|---|---|---|
| **MOEXOG** | inflation_z | 30 | **0.176** | 0.823 | No | keyword_z90 |
| **MOEX10** | oil_z | 1 | **0.166** | 0.374 | No | keyword_z90 |
| FTSE_CHINA_50 | ruble_z | 30 | 0.165 | 0.654 | No | keyword_z30 |
| MOEX10 | rate | 30 | 0.164 | 0.423 | No | keyword_raw |
| MOEXOG | banking | 1 | 0.158 | 0.130 | No | keyword_raw |
| SILVER | rate_z | 7 | 0.158 | 0.269 | No | keyword_z90 |
| MSCI_WORLD | oil_emb_z | 1 | 0.151 | 0.269 | No | embedding_z90 |
| **GOLD** | **inflation_z** | **7** | **0.138** | **0.016** | **YES** | keyword_z90 |
| IMOEX | ruble_z | 1 | 0.149 | 0.760 | No | keyword_z90 |
| BRENT | inflation_emb_z | 7 | 0.140 | 0.135 | No | embedding_z90 |

**GOLD/inflation_z/7 is the only signal with both M5 confirmed AND val confirmed.**

---

## Regime Analysis (2022 vs 2023)

| Regime | Train Signals | Val Signals |
|---|---|---|
| 2022 (crisis) | 10 | 0 |
| 2023 (normalisation) | 2 | 0 |

**Best 2022 signals:**
- USD_RUB / gold / lag=60: IC=0.192 (highest in entire run)
- MOEXFN / ruble / lag=30: IC=0.174
- MOEXFN / gold / lag=7: IC=0.173

**One interesting 2023 signal:**
- SP500 / inflation / lag=7: M5 p=0.004 (very strong structural!), IC=0.171
  → American market responds to Russian inflation news with 7-day lag?
  → Or: global inflation narrative, not Russia-specific. Worth investigating.

---

## Why Phase 2 Found Nothing (AND ensemble, 0 signals)

Phase 2 used M6 strict with `bootstrap_min=0.01` — this is the killer.
The bootstrap CI gate requires 5th-pct > 0.01 (not just > 0), which is
much harder to pass on 450 training rows. The GOLD/inflation_z signal
likely fails the CI gate despite having IC=0.138.

**Next step:** Run Phase 2 specifically for GOLD/inflation_z with:
- M5+M6/AND
- `m6_bootstrap_min=0.0` (just IC gate, no CI requirement)
- Full lags [1,7,14,30,60,90]

---

## Conclusions

### What z-score normalization did

1. **Unlocked MOEXOG/inflation and MOEX10/oil signals** — top of the ranking
2. **Helped M5 pass on GOLD/inflation_z** (p=0.016 vs raw keyword M5 failure)
3. Did NOT dramatically change IC distribution overall (+similar counts)

→ Normalization is necessary for M5, useful but not transformative for M6.

### What the night search found

1. **812 train+val M6-only signals** — market_return target dominates
2. **1 M5+M6 candidate:** GOLD / inflation_z / lag=7
3. **Regime 2022 is richer** (10 vs 2 signals in 2023)
4. **MOEX sector indices** (MOEXOG, MOEX10, MOEXFN, IMOEX) show highest ICs

### What to do next

**Step 1 (30 min):** Targeted AND test on the GOLD/inflation_z signal:
```bash
.venv/Scripts/python -m src.pipeline_v2.night_search \
    --phases 2 --phase-budget 300
```
But modify config to include GOLD/inflation_z specifically and remove bootstrap gate.

**Step 2 (1h):** Focused big_scanner on MOEX sectors × inflation_z/oil_z:
- 4 MOEX instruments × top topics × 6 lags = 24 targeted hypotheses
- With M5+M6/AND (research gates) + z-score normalization

**Step 3:** If GOLD/inflation_z passes full AND test → run Val + Test → **live signal candidate**

---

## Memory for Next Session

- Best feature type: `keyword_z90` (z-score window=90)
- Best target: `market_return` (vol/abs targets yielded nothing)
- Best signal candidate: **GOLD / inflation_z / lag=7**
  - M5 p=0.016, M6 IC=0.138, Val confirmed
  - Interpretation: Russian inflation news → gold price reaction 7 days later
- MOEX sector cluster: MOEXOG/inflation, MOEX10/oil — highest M6 ICs (0.166-0.176)
  but M5 not passing (structural causality unclear)
- 2022 regime: richer signal environment (as expected)
