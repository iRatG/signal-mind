---
type: attack
status: open
target_signals: [signal_usd_rub_moexfn, signal_brent_moexfn, signal_msci_india_moexfn]
severity: critical
closes_with: phase_10_holdout_protocol
tags: [methodology, overfitting, statistics]
updated: 2026-05-02
---

# Attack: In-Sample Overfitting

## Суть атаки
Все сигналы найдены и верифицированы на одних и тех же данных 2022–2025.
Из 3416 гипотез ≈171 дадут r > 0.5 случайно (при α=0.05).

## Формально
D = данные 2022–2025. Мы проверили: сигнал работает на D? → Да.
Мы НЕ проверили: работает ли на holdout D' (2024–2025)?

## Количественная оценка
- 3416 тестов × α=0.05 = **171 ожидаемых ложных позитивов**
- Реальных confirmed ~20% = ~683
- Из них до 25% могут быть ложными

## Что нужно для закрытия
Phase 10: Data split protocol
- Discovery set: 2022–2023 (генерация гипотез)
- Validation set: 2024 (первичная верификация)
- Live set: 2025 (финальная, используется один раз)

Критерий: сигнал работает на Validation с r > 0.5, n > 200.

## Статус: **OPEN — P0**
