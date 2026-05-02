---
type: approach
status: active
marathon_introduced: 2
tags: [methodology, sql, quality]
updated: 2026-05-02
---

# Approach: Aliasing Detection

## Суть
Проверка SQL перед выполнением: не использует ли агент одну и ту же колонку под двумя разными именами в CORR().

## Проблема
```sql
-- ОШИБКА: imoex_close переименован в ftse_china_50
SELECT CORR(imoex_close, imoex_close AS ftse_china_50) -- r = 0.88 by construction
```

## Решение
`revizor.check_aliasing()` — парсит SQL AST, ищет идентичные source columns в CORR().

## Масштаб до внедрения
Marathon 1: до 60% "confirmed" сигналов были aliasing артефактами.

## Файл
`src/agent/revizor.py` → `check_aliasing()`

## Связанные атаки
- [[attack_llm_eval_inflation]] — aliasing был главной причиной инфляции confirmed rate
