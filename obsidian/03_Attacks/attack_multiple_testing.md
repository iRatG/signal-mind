---
type: attack
status: open
target_signals: [all]
severity: major
closes_with: bonferroni_correction + holdout_validation
tags: [methodology, statistics, multiple_testing]
updated: 2026-05-02
---

# Attack: Multiple Testing Problem

## Суть атаки
При 3416 тестах и α=0.05: ожидаемо 171 ложных позитивов по случайности.
Без коррекции на множественное тестирование нельзя доверять ни одному p-value.

## Формально
Если H0: нет корреляции, и мы тестируем M гипотез с порогом α:
Ожидаемое число ложных позитивов = M × α = 3416 × 0.05 = **171**.

## Bonferroni коррекция
Скорректированный порог: α* = α/M = 0.05/3416 = **0.0000146**
Для n=990: это требует |r| > ~0.20 для значимости.

## Более мягкие коррекции
- Benjamini-Hochberg FDR: контролирует долю ложных позитивов
- Bonferroni слишком консервативна для зависимых тестов

## Что нужно для закрытия
1. Применить BH-коррекцию к confirmed сигналам
2. Holdout validation независимо подтверждает (закрывает совместно с [[attack_in_sample_overfitting]])

## Статус: **OPEN — P1**
