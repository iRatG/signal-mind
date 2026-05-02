---
type: decision
status: planned
decision_date: 2026-05-02
decision_maker: iRatG + Claude
alternatives_considered: [random_split, rolling_window, no_split]
tags: [architecture, validation, statistics, phase_10]
updated: 2026-05-02
---

# Decision: Data Split — Walk-Forward Protocol (Phase 10)

## Что решили
Разделить данные на три непересекающихся временных периода.
Агент ищет гипотезы только на Discovery set. Revizor проверяет на Validation.

```
Discovery   2022-01-01 → 2023-12-31   (агент генерирует и проверяет)
Validation  2024-01-01 → 2024-12-31   (Revizor верифицирует каждый confirmed)
Live        2025-01-01 → текущий      (используется ОДИН РАЗ, финальный тест)
```

## Почему именно такой split

### Почему 2022 как начало
- market_data (MSCI, SP500, SILVER и др.) доступны с 2022-01-04
- Ключевые события 2022 (санкции, мобилизация) = реальная рыночная среда для изучения

### Почему 2024 как Validation
- Достаточный n: 252 торговых дня
- Отдельный рыночный режим: ставка выросла до 16→21%, рубль 87-92
- Уже доказало ценность: USD/RUB→MOEXFN не прошёл, Silver→RUGOLD прошёл

### Почему не random split
Random split для временных рядов = data leakage. Будущее попадает в обучение.
Walk-forward (хронологический) — единственный корректный метод для финансовых данных.

### Почему не rolling window
Rolling window усложняет интерпретацию. Walk-forward проще и понятнее для объяснения.

## Техническая реализация

### Шаг 1: Views в DuckDB (минимальный код)
```sql
-- В init_db.py или отдельный скрипт
CREATE OR REPLACE VIEW v_discovery_context AS
  SELECT * FROM v_market_context WHERE trade_date < '2024-01-01';

CREATE OR REPLACE VIEW v_validation_context AS
  SELECT * FROM v_market_context
  WHERE trade_date BETWEEN '2024-01-01' AND '2024-12-31';

CREATE OR REPLACE VIEW v_discovery_news AS
  SELECT * FROM news_daily WHERE news_date < '2024-01-01';
-- (аналогично для moex_sectors, market_data)
```

### Шаг 2: Агент в Discovery mode
В `schema.py` — добавить явный комментарий:
```
-- DISCOVERY MODE: use data from 2022-01-01 to 2023-12-31 only
-- Do NOT query dates >= 2024-01-01 in hypothesis SQL
```
В `hypothesis.py` — добавить проверку: если в SQL есть дата после 2024 — предупреждение.

### Шаг 3: Revizor Validation check
В `revizor.py` — для каждого confirmed сигнала перезапускать SQL с подстановкой validation дат:
```python
def _validate_on_holdout(sql: str, discovery_end='2023-12-31', validation_start='2024-01-01', validation_end='2024-12-31'):
    sql_val = sql.replace("'2022-01-01'", f"'{validation_start}'")
               .replace("'2023-12-31'", f"'{validation_end}'")
    r_val = execute_and_extract_r(sql_val)
    return r_val
```
Сигнал помечается `holdout_pass=True` если r_validation >= 0.5 × r_discovery.

## Граничный случай: лаги
Лаг 90 дней на краю Discovery (2023-12-31) означает что данные нужны до 2024-03-31.
Решение: при lag=90d discovery_end сдвигается на 2023-09-30 (буфер 90 дней).

## Ожидаемый эффект

После Phase 10 у каждого сигнала будет:
```
r_discovery: 0.83    # что агент нашёл
r_validation: -0.47  # реальное качество  ← КЛЮЧЕВАЯ МЕТРИКА
holdout_pass: False
```

На основе holdout_pass:
- True → сигнал в "verified" пул, можно рассматривать для применения
- False → в "invalidated", агент его не повторяет

## Открытые вопросы
- Как обработать сигналы условные на режим (rate < 15%)? В 2024 rate > 15%, значит сигнал не должен оцениваться. Нужен "режимный holdout": найти период когда режим совпадает.
- Validation set = всего 1 год. Если 2024 был аномальным годом — false negatives.
- Silver→RUGOLD: нет данных 2022, Discovery только 2023. Нужно больше данных.
