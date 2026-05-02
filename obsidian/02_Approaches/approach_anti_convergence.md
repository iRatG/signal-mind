---
type: approach
status: active
marathon_introduced: 3
tags: [methodology, convergence, diversity]
updated: 2026-05-02
---

# Approach: Anti-Convergence

## Суть
Агент склонен возвращаться к одним и тем же "успешным" топикам → convergence trap.
Anti-convergence механизм принудительно расширяет область поиска.

## Механизмы
1. **convergence_blacklist.json** — топики с WEAK_R > N раз → автоматический blacklist
2. **RANDOM_JUMP_PROB = 0.25** — 25% итераций случайная смена темы
3. **TOPIC_POOL ротация** — принудительная смена каждые K итераций

## Почему важно
Без anti-convergence система быстро "застывает" на 3–5 сигналах и перестаёт исследовать пространство гипотез.

## Связь с полным скоупом
[[approach_full_scope_sweep]] — расширяет anti-convergence до уровня инструментов:
не просто новые топики, но и нетронутые индексы/инструменты.

## Файл
`src/agent/agent.py` → RANDOM_JUMP_PROB, convergence_blacklist

## Связанные атаки
- [[attack_scope_convergence]] — convergence на подмножестве данных
