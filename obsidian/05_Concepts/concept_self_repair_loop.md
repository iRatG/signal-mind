---
type: concept
status: active
tags: [concept, sql, repair, architecture]
updated: 2026-05-02
---

# Concept: SQL Self-Repair Loop

## Definition
Петля 1 Ouroboros: агент получает SQL ошибку → классифицирует тип → применяет исправление → повторяет.

## 8 классов ошибок
1. COLUMN_NOT_FOUND — колонка отсутствует в схеме
2. TABLE_NOT_FOUND — таблица не существует
3. SYNTAX_ERROR — синтаксис SQL неверен
4. ALIASING — инструмент переименован некорректно
5. TYPE_MISMATCH — несовместимые типы данных
6. JOIN_ERROR — неверный ключ JOIN
7. AGG_ERROR — агрегатная функция применена неверно
8. TIMEOUT — запрос слишком долгий

## Файл
`src/agent/sql_repair.py`

## Статистика
3416 итераций: repair loop предотвратил сотни failed iterations.
Success rate repair: ~85% с первой попытки.

## Связанные файлы
- `db/sql_patterns.md` — рабочие SQL шаблоны
- `db/forbidden_patterns.md` — анти-паттерны
