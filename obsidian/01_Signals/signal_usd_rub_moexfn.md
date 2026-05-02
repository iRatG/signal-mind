---
type: signal
status: under_attack
instrument_a: USD_RUB
instrument_b: MOEXFN
lag_days: 14
r: 0.758
n: 990
confidence: medium
marathon_found: 1
tags: [signal, forex, banking, moexfn]
updated: 2026-05-02
---

# Signal: USD/RUB → MOEXFN (lag 14d)

## Summary
Рост USD/RUB (ослабление рубля) коррелирует с ростом финансового индекса MOEX через 14 дней.
r = +0.758, n = 990, период 2022–2025. Один из трёх сильнейших сигналов системы.
**Статус: под атакой** — 2024 инверсия не объяснена, holdout не проведён.

## Статистика
- r: +0.758
- lag: 14 дней
- n: 990
- Период: 2022–2025
- Marathon found: 1 (2026-04-30)
- Подтверждений: 3+

## Стабильность по годам
| Год | r | n | Статус |
|-----|---|---|--------|
| 2022 | +0.81 | ~250 | ✅ сильный |
| 2023 | +0.96 | ~250 | ✅ очень сильный |
| 2024 | **-0.47** | ~250 | ❌ инверсия |
| 2025 | +0.34 | ~240 | ⚠️ слабый |

## Экономический механизм
Ослабление рубля → импортная инфляция → ЦБ удерживает высокую ставку →
банки зарабатывают на процентных доходах (NIM растёт) → MOEXFN растёт.
При ставке 21%+ механизм перегружается → возможна инверсия (кредитный стресс).

## SQL верификация
```sql
WITH lagged AS (
    SELECT m1.trade_date, m1.usd_rub, m2.moexfn_finance
    FROM v_market_context m1
    JOIN v_moex_sectors m2 ON m2.trade_date = m1.trade_date + INTERVAL 14 DAYS
    WHERE m1.usd_rub IS NOT NULL AND m2.moexfn_finance IS NOT NULL
      AND m1.trade_date BETWEEN '2022-01-01' AND '2025-12-31'
)
SELECT ROUND(CORR(usd_rub, moexfn_finance), 4) AS r, COUNT(*) AS n FROM lagged;
-- r = 0.7580, n = 990
```

## Открытые атаки
- [[attack_non_stationarity]] — 2024 инверсия r=-0.47 НЕ объяснена ← **P0**
- [[attack_in_sample_overfitting]] — нет holdout теста ← **P0**
- [[attack_regime_conditionality]] — работает ли при ставке < 15%?

## Закрытые атаки
- aliasing — проверено: usd_rub берётся корректно ✅

## Связанные концепты
- [[concept_regime_conditionality]]
- [[concept_signal_nonstationarity]]

## Связанные сигналы
- [[signal_brent_moexfn]] — через нефть влияет на тот же сектор
- [[signal_moexog_usd_rub]] — USD/RUB связан с нефтянкой через MOEXOG

## Open questions
- Насколько сигнал сохранится при снижении ставки ниже 10%?
- Есть ли нелинейный эффект при USD/RUB > 100?
- Почему 2024 дал инверсию — административные интервенции?
