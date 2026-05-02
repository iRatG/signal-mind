---
type: signal
status: promising
instrument_a: SILVER
instrument_b: RUGOLD
lag_days: 30
r_full_sample: 0.9015
n_full_sample: 443
r_discovery: 0.1045
r_validation_2024: 0.7201
r_validation_2025: 0.9197
date_found: 2026-05-02
marathon: 5
confidence: medium
tags: [signal, commodities, gold, silver, rugold]
updated: 2026-05-02
---

# Signal: SILVER → RUGOLD (lag 30d)

## Суть
Рост цены серебра (SILVER) коррелирует с ростом российского золотого индекса RUGOLD через 30 дней.
r = +0.90 full sample (2023–2025), n = 443.

**Особенность:** единственный сигнал системы который УСИЛИВАЕТСЯ out-of-sample, а не слабеет.

## SQL верификация (live DuckDB)
```sql
WITH lagged AS (
    SELECT m1.trade_date,
           m1.close AS silver,
           s.rugold
    FROM market_data m1
    JOIN v_moex_sectors s ON s.trade_date = m1.trade_date + INTERVAL 30 DAYS
    WHERE m1.instrument = 'SILVER'
      AND s.rugold IS NOT NULL
      AND m1.trade_date BETWEEN '2022-01-01' AND '2025-12-31'
)
SELECT ROUND(CORR(silver, rugold), 4) AS r, COUNT(*) AS n FROM lagged;
-- r = 0.9015, n = 443
```

## Стабильность по годам
| Год | r | n | Статус |
|-----|---|---|--------|
| 2022 | нет данных | 0 | — (SILVER нет в market_data до 2023) |
| 2023 | +0.10 | 142 | ⚠️ слабый (Discovery период) |
| 2024 | **+0.72** | 148 | ✅ сильный (Validation!) |
| 2025 | **+0.92** | 153 | ✅ очень сильный (Live!) |

## Почему это важно
Все остальные сильные сигналы (USD/RUB→MOEXFN, Brent→MOEXFN) **инвертируются** или исчезают в 2024.
Silver→RUGOLD — исключение: сигнал нарастает. Возможные причины:
- Серебро = глобальный индикатор спроса на промышленные металлы → влияет на золотодобычу
- Российские золотодобытчики экспортируют, поэтому мировая цена серебра (как proxy commodities) опережает их оценку
- Лаг 30 дней = время от получения ценового сигнала до пересмотра рынком справедливой стоимости RUGOLD

## Открытые атаки
- [[attack_in_sample_overfitting]] — 2022 данных нет, только 2 года Discovery, мало для вывода ← P1
- [[attack_multiple_testing]] — из 618+ комбинаций могло попасть случайно ← P1
- Нет данных при сильном рубле (USD/RUB < 70): неизвестно как ведёт себя

## Закрытые атаки
- Out-of-sample: ПРОХОДИТ — 2024 и 2025 сильнее 2023 ✅

## Связанные сигналы
- [[signal_brent_moexfn]] — другой commodity → Russian market сигнал (но НЕ прошёл holdout)
- [[signal_moexog_usd_rub]] — нефтянка через похожий механизм

## Следующие шаги
1. Проверить без условия слабого рубля — работает ли сигнал всегда?
2. Проверить Silver → MOEXOG (не только RUGOLD)
3. Дождаться 2026 данных — подтвердить тренд усиления
