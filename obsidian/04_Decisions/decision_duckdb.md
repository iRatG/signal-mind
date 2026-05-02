---
type: decision
status: active
decision_date: 2026-04-15
alternatives_considered: [PostgreSQL, ClickHouse, pandas+parquet]
tags: [architecture, database]
updated: 2026-05-02
---

# Decision: DuckDB как основная аналитическая БД

## Что решили
Все числовые данные хранятся в DuckDB (`db/signal_mind.duckdb`).
Агент выполняет SQL-запросы напрямую к файлу.

## Почему DuckDB
| Критерий | DuckDB | PostgreSQL | pandas |
|----------|--------|-----------|--------|
| Latency на запрос | **40 мс** | 200+ мс | 2–10 с |
| Деплой | один файл | сервер | в памяти |
| CORR(), window functions | ✅ native | ✅ | ограничено |
| Наш объём данных | идеально | избыточно | норм |

40 мс на запрос критично — агент делает 10–20 SQL/итерацию.

## Ограничения (приняты)
- Не подходит для конкурентной записи (агент только читает — ок)
- Максимум ~100 GB (у нас ~7 MB числовых данных — ок)
- Новостная БД отдельно в SQLite (9.93 GB, DuckDB не оптимален для full-text)
