---
type: approach
status: active
marathon_introduced: 1
tags: [methodology, architecture]
updated: 2026-05-02
---

# Approach: Ouroboros Loop — три петли обучения

## Суть
Самореферентный цикл: агент генерирует гипотезы → верифицирует SQL → оценивает → пополняет базу знаний → следующая гипотеза использует накопленное знание.

## Три петли
```
Петля 1 (per-iteration): SQL self-repair
  ошибка DuckDB → классификация (8 типов) → автоисправление

Петля 2 (per-session): Ouroboros hypothesis cycle
  гипотеза → SQL → данные → оценка → новая гипотеза

Петля 3 (cross-session): накопление датасета
  experiments.db → fine-tuning DeepSeek
```

## Петля 4 (NEW — Methodology RAG)
```
vault обновляется после итерации → ChromaDB переиндексируется →
агент запрашивает vault перед следующей гипотезой →
более осознанная гипотеза с учётом истории
```

## Параметры агента
- LAG_SWEEP: [0, 7, 14, 0, 30, 0, 60, 0, 90, 0]
- REFLECT_EVERY: 5 итераций
- RANDOM_JUMP_PROB: 0.25

## Почему работает
Каждый цикл добавляет знание в систему. Даже "неудачные" итерации (rejected) обогащают base через forbidden_patterns и experiments.db.

## Связанные файлы
- `src/agent/agent.py` — основной цикл
- `src/agent/hypothesis.py` — генерация + оценка
- [[approach_llm_self_eval]] — устаревший подход оценки (deprecated)
