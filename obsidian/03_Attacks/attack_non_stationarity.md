---
type: attack
status: open
target_signals: [signal_usd_rub_moexfn, signal_brent_moexfn]
severity: critical
closes_with: regime_conditional_analysis + rolling_window
tags: [methodology, stationarity, statistics]
updated: 2026-05-02
---

# Attack: Non-Stationarity

## Суть атаки
Коэффициент корреляции меняется по годам. USD/RUB → MOEXFN:
2023: r=+0.96 → 2024: r=**-0.47** (инверсия!).
Значит сигнал нестабилен → не воспроизводим в будущем.

## Доказательство
| Год | r (USD/RUB → MOEXFN, lag 14d) |
|-----|-------------------------------|
| 2022 | +0.81 |
| 2023 | +0.96 |
| 2024 | **-0.47** ❌ |
| 2025 | +0.34 |

## Почему возникает
Механизм меняется при разных режимах:
- 2022–2023: ставка 7–15%, механизм работает
- 2024: ставка 16–21%, mechanism breakdown (кредитный стресс)
- Возможно: административные интервенции в курс USD/RUB в 2024

## Что нужно для закрытия
1. Rolling window correlation (12-month window) → визуализация нестабильности
2. Regime-conditional analysis: разделить по режимам ставки
3. Структурный тест на break point (Chow test или CUSUM)

## Статус: **OPEN — P0**

## Связанные концепты
- [[concept_regime_conditionality]]
- [[concept_signal_nonstationarity]]
