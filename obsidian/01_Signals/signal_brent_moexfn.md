---
type: signal
status: under_attack
instrument_a: BRENT
instrument_b: MOEXFN
lag_days: 90
r: 0.71
n: 850
confidence: medium
marathon_found: 1
tags: [signal, oil, banking, moexfn]
updated: 2026-05-02
---

# Signal: Brent → MOEXFN (lag 90d)

## Summary
Цена Brent предсказывает финансовый индекс MOEX через 90 дней.
r = +0.71, n ≈ 850. Косвенный механизм через нефтяные доходы государства и банковский сектор.
**Статус: под атакой** — нет holdout теста, режимная зависимость не проверена.

## Статистика
- r_full_sample: -0.702 (negative — нефть↑ → MOEXFN↓ через 90d)
- r_discovery (2022-2023): **-0.725**, n=401
- r_validation_2024: **-0.097**, n=199 — сигнал исчез
- r_validation_2025: **+0.265**, n=199 — знак инвертировался
- lag: 90 дней
- Marathon found: 1

## ⚠️ Вердикт holdout (2026-05-02)
**Сигнал НЕ прошёл holdout.** r=-0.72 в Discovery → r=-0.10 в 2024, знак меняется в 2025.
Возможная причина: с 2022 Россия продаёт нефть по дисконту Urals→Brent ($15-25),
поэтому мировая цена Brent всё меньше отражает реальные доходы российского бюджета.

## Экономический механизм
Рост нефти → рост доходов бюджета → государство наращивает расходы →
деньги попадают в экономику → банки получают больше депозитов и кредитов →
MOEXFN растёт через 90 дней.

Альтернативный механизм: нефть → MOEXOG → дивиденды → перетекание в MOEXFN.

## SQL верификация
```sql
WITH lagged AS (
    SELECT m1.trade_date, m1.brent_usd, m2.moexfn_finance
    FROM v_market_context m1
    JOIN v_moex_sectors m2 ON m2.trade_date = m1.trade_date + INTERVAL 90 DAYS
    WHERE m1.brent_usd IS NOT NULL AND m2.moexfn_finance IS NOT NULL
      AND m1.trade_date BETWEEN '2022-01-01' AND '2024-06-30'
)
SELECT ROUND(CORR(brent_usd, moexfn_finance), 4) AS r, COUNT(*) AS n FROM lagged;
```

## Открытые атаки
- [[attack_in_sample_overfitting]] — **ПОДТВЕРЖДЕНА**: сигнал исчез на holdout 2024 ← **P0 CRITICAL**
- [[attack_non_stationarity]] — знак меняется год от года ← **P0 CRITICAL**
- [[attack_regime_conditionality]] — дисконт Urals/Brent делает Brent плохим прокси с 2022

## Связанные концепты
- [[concept_regime_conditionality]]
- [[concept_lag_hypothesis]]

## Связанные сигналы
- [[signal_usd_rub_moexfn]] — оба через макроэкономику влияют на банки
- [[signal_oil_news_moexog]] — нефтяные новости → нефтяной сектор (более прямая связь)

## Open questions
- Lag 90d очень длинный — случайна ли корреляция?
- Есть ли более сильный сигнал на lag 30d или 60d?
- Влияние дисконта Urals к Brent после 2022?
