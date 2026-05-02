---
type: attack
status: partially_closed
target_signals: [signal_usd_rub_moexfn, signal_brent_moexfn, signal_msci_india_moexfn, signal_silver_rugold]
severity: critical
closes_with: phase_10_holdout_protocol
tags: [methodology, overfitting, statistics]
updated: 2026-05-02
---

# Attack: In-Sample Overfitting

## Суть атаки
Все сигналы найдены и верифицированы на одних и тех же данных 2022–2025.
Из 3486+ гипотез ≈174 дадут r > 0.5 случайно (при α=0.05).

## Формально
D = данные 2022–2025. Мы проверили: сигнал работает на D? → Да.
Мы НЕ проверили: работает ли на holdout D' (2024–2025)?

## Количественная оценка
- 3486 тестов × α=0.05 = **174 ожидаемых ложных позитивов**
- Реальных confirmed ~20% = ~697
- Из них до 25% могут быть ложными

## ⚡ Результаты первого holdout теста (2026-05-02)

Discovery = 2022–2023, Validation = 2024, Live = 2025:

| Сигнал | Discovery r | Validation 2024 r | Live 2025 r | Вердикт |
|--------|------------|-------------------|-------------|---------|
| USD/RUB → MOEXFN (14d) | +0.83 | **-0.47** | +0.40 | ❌ НЕ ПРОШЁЛ |
| Brent → MOEXFN (90d) | -0.73 | **-0.10** | +0.27 | ❌ НЕ ПРОШЁЛ |
| Silver → RUGOLD (30d) | +0.10 | **+0.72** | **+0.92** | ✅ ПРОШЁЛ |

**Вывод:** два из трёх "сильнейших" сигналов системы — артефакты периода 2022–2023.
Silver→RUGOLD — единственный, который усиливается out-of-sample.

## Что нужно для полного закрытия
Phase 10: Data split protocol — официально разделить данные и запустить агента только на Discovery set.

Критерий закрытия: сигнал имеет r > 0.5 на Validation set при n > 150.

## Статус: **PARTIALLY CLOSED**
Holdout тест проведён вручную. Формальный split в агенте не реализован.
Нужен Phase 10 чтобы агент сам работал только на Discovery данных.
