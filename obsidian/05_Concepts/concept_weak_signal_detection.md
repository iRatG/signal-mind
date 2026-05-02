---
type: concept
status: active
tags: [concept, signal, methodology]
updated: 2026-05-02
---

# Concept: Weak Signal Detection

## Definition
Слабый сигнал: 0.30 ≤ r < 0.50, достаточная выборка (n ≥ 200).
Сильный сигнал: r ≥ 0.50.

## Почему слабые сигналы важны
Рынки эффективны — сильные очевидные связи быстро арбитражируются.
Слабые сигналы (r=0.3–0.5) могут быть реальными, но режимно-зависимыми.

## Пороги системы
- WEAK_R threshold: r < 0.30 → auto-blacklist (Revizor)
- Partial signal: 0.30 ≤ r < 0.50
- Confirmed: r ≥ 0.50 + n ≥ 200

## Ловушка
Слабый сигнал в режиме A может быть сильным в режиме B.
Не отбрасывать r=0.25 без режимного анализа.

## Связанные концепты
- [[concept_regime_conditionality]] — режимный анализ усиливает слабые сигналы
- [[concept_lag_hypothesis]] — оптимальный лаг тоже влияет на r
