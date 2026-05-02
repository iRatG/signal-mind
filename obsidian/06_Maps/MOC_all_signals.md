---
type: moc
status: active
tags: [moc, signals]
updated: 2026-05-02
---

# MOC: All Signals — карта всех найденных сигналов

## Confirmed / Under Attack (r ≥ 0.50)

| Сигнал | r | Лаг | Открытые атаки |
|--------|---|-----|----------------|
| [[signal_usd_rub_moexfn]] | 0.758 | 14d | overfitting, non-stationarity |
| [[signal_brent_moexfn]] | 0.71 | 90d | overfitting, non-stationarity |
| [[signal_msci_india_moexfn]] | ~0.65 | 0d | overfitting, mechanism unclear |
| [[signal_moexog_usd_rub]] | 0.70 | 0d | reverse causality unclear |

## Partial (0.30 ≤ r < 0.50)

| Сигнал | r | Лаг | Условие |
|--------|---|-----|---------|
| [[signal_oil_news_moexog]] | 0.16 | 30d | слабый, нужен режим |
| [[signal_sanctions_usd_rub]] | 0.43 | 7d | только при USD/RUB > 80 |
| [[signal_inflation_moex10]] | -0.34 | 14d | только при inflation > 8% |

## Not Tested — ПРИОРИТЕТЫ

### P0 — критически важно
- RUCBTR3YNS/5Y → MOEXFN (bond-equity, lag 0–14d)
- ALUMINUM → MOEXOG (metal → energy, lag 0–30d)
- RUSFAR3M → MOEXFN (money market rate → banking, lag 0–7d)

### P1
- SILVER → RUGOLD (cross-commodity, lag 0d)
- news_gold → RUGOLD (topic → sector, lag 7–14d)
- CHINA_H_SHARES → BRENT (demand effect, lag 30–90d)
- MREDC → ? (недвижимость как цель — нет ни одного сигнала)

### P2
- DJ_SOUTH_AFRICA → IMOEX (EM diversification)
- DXY → USD_RUB (lag 7–14d через глобальные потоки)
- days_to_meeting → MOEXFN (pre-CBR meeting effect)

## Статистика покрытия
- Всего потенциальных пар: ~200+
- Активно тестировалось: ~50 (25%)
- Confirmed/partial: 7 (3.5%)
- Нетронуто: ~150 (75%) ← **здесь могут быть сигналы**
