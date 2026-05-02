---
type: concept
status: active
tags: [concept, statistics, nonstationarity]
updated: 2026-05-02
---

# Concept: Signal Non-Stationarity

## Definition
Коэффициент корреляции между инструментами меняется во времени.
Сигнал, найденный в 2022–2023, может не работать в 2024–2025 и наоборот.

## Диагностика
- Rolling window correlation (12-month window)
- Year-by-year r breakdown
- Structural break tests (Chow, CUSUM)

## Источники нестационарности
1. Смена монетарного режима (ставка 7% → 21%)
2. Структурные шоки (санкции 2022)
3. Сезонность
4. Изменение состава индексов

## Правило
Любой confirmed сигнал с |Δr| > 0.3 между соседними годами → помечать как `under_attack`.

## Связанные атаки
- [[attack_non_stationarity]]
- [[attack_regime_conditionality]]
