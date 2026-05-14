# Data Audit — Experiment v1

Pre-sample diagnostics across Train / Validation / Test windows.
Computed before any signal-finding starts. Source: signal_mind.duckdb + hf_news.db.

## Windows

| Window | Start | End | Trading days |
|---|---|---|---|
| Train | 2022-01-01 | 2023-09-30 | 455 |
| Validation | 2024-01-01 | 2025-04-30 | 348 |
| Test | 2025-09-01 | 2026-04-29 | 173 |

Buffer zones (excluded, prevent lag-leak between windows):

- `buf_train_val`: 2023-10-01 → 2023-12-31
- `buf_val_test`: 2025-05-01 → 2025-08-31

## Market data integrity

| Field | Train | Validation | Test |
|---|---|---|---|
| n_market_obs | 455 | 348 | 173 |
| missing imoex_close | 6.81% | 3.45% | 3.47% |
| missing usd_rub | 0.0% | 0.0% | 0.0% |
| missing brent_usd | 0.88% | 1.44% | 1.73% |
| missing gold_usd | 21.76% | 24.43% | 12.14% |
| missing key_rate_pct | 0.0% | 0.0% | 12.14% |

## News coverage

| Metric | Train | Validation | Test |
|---|---|---|---|
| n_news_total | 1,089,773 | 40,652 | 43,003 |
| lang_share_en | 100.0% | 100.0% | 37.3% |
| lang_share_ru | 0% | 0% | 62.7% |
| lang_share_other | 0% | 0% | 0% |

### Per-topic mentions and day-coverage

| Topic | Train mentions / coverage | Validation mentions / coverage | Test mentions / coverage |
|---|---|---|---|
| oil | 148,414 / 100.0% | 4,079 / 90.3% | 5,961 / 100.0% |
| rate | 94,247 / 100.0% | 2,064 / 59.7% | 3,301 / 100.0% |
| ruble | 541 / 29.3% | 6 / 1.2% | 2,257 / 97.5% |
| sanctions | 8,335 / 90.0% | 263 / 19.1% | 1,653 / 97.9% |
| inflation | 115,963 / 100.0% | 2,158 / 69.3% | 1,239 / 75.1% |
| banking | 42,731 / 99.7% | 1,500 / 41.2% | 2,777 / 99.2% |
| gold | 32,504 / 99.4% | 790 / 51.9% | 1,097 / 96.3% |

**Rule:** if coverage_pct < 30% in any window, the topic is marked unusable in that window.

## Regime metrics

| Metric | Train | Validation | Test |
|---|---|---|---|
| key_rate min/median/max (%) | 7.50 / 8.00 / 20.00 | 16.00 / 18.50 / 21.00 | 15.00 / 16.00 / 17.00 |
| USD/RUB min/median/max | 51.16 / 74.94 / 120.38 | 80.76 / 91.26 / 109.58 | 74.59 / 79.08 / 85.66 |
| IMOEX daily vol (%) | 2.589 | 1.441 | 1.136 |

## Topics disqualified by coverage rule

- **Train**: ruble
- **Validation**: ruble, sanctions
