# Signal Brief — Верифицированные паттерны
**Дата верификации:** 2026-04-30  
**Метод:** Независимый прогон SQL на живой DuckDB с правильными источниками данных  
**Итог верификации:** из 9 кластеров — 2 реальных, 1 условный, 4 фальшивых (ошибка агента), 2 слабых

---

## КРИТИЧЕСКАЯ НАХОДКА: Системная ошибка агента

Агент использовал **неправильные колонки** для 4 кластеров:
- Вместо `market_data WHERE instrument='DXY'` → брал `v_market_context.usd_rub` (переименовывал в "dxy")
- Вместо `market_data WHERE instrument='FTSE_CHINA_50'` → брал `v_market_context.imoex_close`
- Аналогично для MSCI_INDIA и DJ_SOUTH_AFRICA → использовал IMOEX

**Результат:** IMOEX vs MOEXOG = r=0.877 (два российских индекса — конечно коррелируют!).  
Агент "подтверждал" фиктивные сигналы с score=85. Реальный confirmed rate значительно ниже.

**Исправление:** добавлено в schema.py — явный запрет использования v_market_context как прокси для market_data инструментов.

---

## ✅ СИГНАЛ #1 — EUR/RUB → USD/RUB (lag 1 день)

**Статус:** РЕАЛЬНЫЙ, СИЛЬНЫЙ  
**Корреляция:** lag=0: r=1.000 | lag=1d: r=0.9905 | n=1127  
**Данные:** v_market_context, 2022–2026  

**Что это означает:** EUR/RUB вчера почти идеально предсказывает USD/RUB сегодня (r=0.99).  
**Причина:** Обе пары рублёвые, ЦБ управляет ими одновременно. EUR/RUB реагирует чуть быстрее из-за большей ликвидности на MOEX.  
**Ограничение:** Это скорее бухгалтерская тавтология (EUR/USD × USD/RUB ≈ EUR/RUB), чем торговый сигнал. Не даёт прогнозной ценности сам по себе.

```sql
WITH t AS (
    SELECT trade_date, eur_rub, usd_rub,
           LAG(eur_rub) OVER (ORDER BY trade_date) AS eur_lag1
    FROM v_market_context
    WHERE trade_date BETWEEN '2022-01-01' AND '2026-04-30'
      AND eur_rub IS NOT NULL AND usd_rub IS NOT NULL
)
SELECT ROUND(CORR(eur_rub, usd_rub), 4) AS corr_lag0,
       ROUND(CORR(eur_lag1, usd_rub), 4) AS corr_lag1,
       COUNT(*) AS n
FROM t WHERE eur_lag1 IS NOT NULL
-- Result: corr_lag0=1.0000, corr_lag1=0.9905, n=1127
```

---

## ✅ СИГНАЛ #2 — MSCI_INDIA → MOEXFN (lag 7-14 дней)

**Статус:** РЕАЛЬНЫЙ, УМЕРЕННЫЙ  
**Корреляция:** lag=7d: r=0.626 | lag=14d: r=0.625 | n≈1000  
**Данные:** market_data (MSCI_INDIA) + v_moex_sectors (moexfn_finance), 2022–2025  

**Что это означает:** Рост индийского рынка предсказывает рост российского финансового сектора через 7-14 дней.  
**Причина:** Оба — развивающиеся рынки с высокой долей сырьевого сектора. Глобальный аппетит к EM-активам коррелирует.  
**Нюанс режима:** В сильном рубле (USD/RUB < 80): r=0.77 (n=223). В слабом рубле (USD/RUB > 80): r=0.16 (n=374). Сигнал значимо ослабевает при санкционном давлении на рубль.  
**Вывод:** Полезен как контекстуальный индикатор в периоды нормального курса, но ненадёжен в нынешнем режиме.

```sql
WITH t AS (
    SELECT d.close AS msci_india, s.moexfn_finance, m.usd_rub
    FROM market_data d
    JOIN v_moex_sectors s ON s.trade_date = d.trade_date + INTERVAL 14 DAYS
    JOIN v_market_context m ON m.trade_date = d.trade_date
    WHERE d.instrument = 'MSCI_INDIA'
      AND d.trade_date BETWEEN '2022-01-01' AND '2025-12-31'
      AND d.close IS NOT NULL AND s.moexfn_finance IS NOT NULL
)
SELECT ROUND(CORR(msci_india, moexfn_finance), 4) AS corr, COUNT(*) AS n FROM t
-- Result: corr=0.6252, n=1000
```

---

## ⚠️ СИГНАЛ #3 — Banking news → MOEXFN (lag 14 дней, условный)

**Статус:** РЕАЛЬНЫЙ, НО СЕЙЧАС НЕАКТИВЕН  
**Корреляция:** при ставке < 15%: r=0.549, n=383 | при ставке ≥ 15%: r=0.010, n=355  
**Данные:** news_daily + v_moex_sectors + v_market_context, 2022–2025  

**Что это означает:** Когда ключевая ставка низкая (<15%), новостной фон банковского сектора предсказывает MOEXFN через 2 недели. При высокой ставке (текущей) — сигнал полностью отсутствует.  
**Текущий статус:** Ставка ЦБ ~16-21% → сигнал **НЕАКТИВЕН**.  
**Когда применять:** Мониторить при снижении ставки ниже 15%.

```sql
WITH t AS (
    SELECT n.banking AS banking_mentions, s.moexfn_finance, m.key_rate_pct
    FROM news_daily n
    JOIN v_moex_sectors s ON s.trade_date = n.news_date + INTERVAL 14 DAYS
    JOIN v_market_context m ON m.trade_date = n.news_date
    WHERE n.news_date BETWEEN '2022-01-01' AND '2025-01-01'
      AND n.banking IS NOT NULL AND s.moexfn_finance IS NOT NULL
),
regimes AS (
    SELECT banking_mentions, moexfn_finance,
           CASE WHEN key_rate_pct < 15 THEN 'low_rate' ELSE 'high_rate' END AS regime
    FROM t WHERE key_rate_pct IS NOT NULL
)
SELECT regime, ROUND(CORR(banking_mentions, moexfn_finance), 4) AS corr, COUNT(*) AS n
FROM regimes GROUP BY regime
-- Result: low_rate r=0.5489 n=383 | high_rate r=0.0095 n=355
```

---

## ❌ ФАЛЬШИВЫЕ СИГНАЛЫ (ошибка агента — неправильные колонки)

| Сигнал | Что агент мерил | Реальный результат |
|--------|-----------------|-------------------|
| DXY → USD/RUB lag 7d | USD/RUB → USD/RUB (тавтология) | r=-0.087, n=1112 — нет сигнала |
| FTSE_CHINA_50 → MOEXOG | IMOEX → MOEXOG (оба российские) | r=-0.17 до -0.32 — обратная связь |
| DJ_SOUTH_AFRICA → MOEXOG | IMOEX → MOEXOG | r=-0.59 (слабый рубль), r=+0.45 (сильный рубль) |
| MSCI_INDIA → MOEXOG | IMOEX → MOEXOG | r=0.40 — умеренная (но это был не MSCI_INDIA) |

---

## 📊 ИНТЕРЕСНАЯ НАХОДКА: Обратная связь иностранные рынки → MOEXOG

**При слабом рубле (USD/RUB > 80, текущий режим):**
- FTSE_CHINA_50 → MOEXOG lag 14d: r = **-0.56** (n=569)
- DJ_SOUTH_AFRICA → MOEXOG lag 7d: r = **-0.59** (n=602)

Рост иностранных emerging markets **отрицательно** коррелирует с российским нефтяным сектором в режиме слабого рубля.

**Гипотеза о причине:** Рост EM-рынков = глобальный аппетит к риску = укрепление рубля → MOEXOG в рублях падает (курсовой эффект). Или: рост Китая/ЮА рынков = высокий спрос на нефть → цена Brent растёт → рубль укрепляется → MOEXOG в рублях компенсируется.  
**Статус:** Требует дополнительного исследования.

---

## 🔴 СЛАБЫЕ СИГНАЛЫ (статистически незначимы)

| Сигнал | Корреляция | n | Вывод |
|--------|-----------|---|-------|
| DXY (реальный) → USD/RUB | r=-0.07 до -0.12 | 600-1100 | Нет сигнала. USD/RUB отвязан от DXY из-за санкций/КК |
| Oil news → MOEXOG (все лаги) | r=0.11-0.12 | ~758 | Слишком слабо |
| Rate news → MOEXFN | r=0.11 | 758 | Слишком слабо |
| Sanctions news → MOEX10 | r=-0.08 | 744 | Нет сигнала |

---

---

## ЧИСТЫЙ СКАН — систематический перебор (signal_scan.py)

**Метод:** 618 комбинаций (10 внешних инструментов + 7 новостных тем + 5 макро) × 5 целей × 6 лагов.
Каждый источник берётся строго из правильной таблицы. Минимум n=150.

### СИЛЬНЫЕ сигналы (|r| ≥ 0.60, верифицированы)

| Сигнал | r | lag | n | Механизм |
|--------|---|-----|---|----------|
| USD/RUB → MOEXFN | +0.758 | 14d | 990 | Девальвация → переоценка активов в рублях вверх |
| USD/RUB → MOEX10 | +0.743 | 14d | 990 | То же, голубые фишки |
| USD/RUB → IMOEX  | +0.757 | 60d | 595 | Курс рубля определяет рынок с лагом 2 месяца |
| **Brent → MOEXFN** | **-0.702** | **90d** | **799** | Рост нефти → укрепление рубля → сжатие кредитования → MOEXFN вниз |
| KEY_RATE → MOEXFN | +0.651 | 0d  | 992 | Банки зарабатывают на высокой марже при высоких ставках |
| MSCI_INDIA → MOEXFN | +0.631 | 0d | 986 | Глобальный аппетит к EM двигает оба рынка |

### УМЕРЕННЫЕ сигналы (0.35 ≤ |r| < 0.60)

| Сигнал | r | lag | n |
|--------|---|-----|---|
| MSCI_WORLD → MOEXFN | +0.599 | 0d  | 992 |
| SP500 → MOEXFN      | +0.593 | 7d  | 987 |
| Brent → USD/RUB     | -0.564 | 90d | 827 |
| GOLD → MOEXFN       | +0.434 | 60d | 378 |
| SILVER → MOEXFN     | +0.366 | 14d | 760 |

### Слабые но устойчивые (0.20–0.35)

- DXY → MOEXFN: r=-0.346 (рост доллара → российский финсектор вниз)
- Sanctions news → USD/RUB lag 90d: r=-0.233 (слабый, но направление логично)
- news_gold → MOEXOG lag 60d: r=+0.226

---

## Выводы для следующего прогона агента

1. **Исправить schema.py** — добавлен явный запрет использовать v_market_context как прокси для DXY/FTSE/MSCI/DJ (уже внесено)
2. **Фокус следующего прогона:** проверить MSCI_INDIA → MOEXFN в разных режимах подробнее; исследовать почему иностранные EM отрицательно коррелируют с MOEXOG
3. **Реальный confirmed rate:** значительно ниже 67.7% — большинство "confirmed" были артефактами неверных колонок. Честная оценка ~15-25%.
4. **Ценная находка:** Режимный анализ (слабый/сильный рубль) реально меняет знак корреляции — это само по себе торгово-значимая информация.
