---
type: concept
status: active
tags: [concept, regime, methodology]
updated: 2026-05-02
---

# Concept: Regime Conditionality

## Definition
Сигнал работает только в определённом рыночном режиме. Средний r по всему периоду маскирует
сильный r в одном режиме и нулевой/негативный в другом.

## Текущие режимы системы
```json
{
  "rate_hike":    {"key_rate_pct": ">= 15"},
  "high_inflation": {"inflation_pct": ">= 8"},
  "weak_ruble":   {"usd_rub": ">= 80"},
  "bear_market":  {"imoex_trend_3m": "<= -10"}
}
```

## Примеры
- USD/RUB → MOEXFN: r=+0.96 в 2023 (rate_hike), r=-0.47 в 2024 (rate_overshoot)
- Sanctions → USD/RUB: r=0.43 при weak_ruble, ~0.15 без условия
- Inflation → MOEX10: r=-0.34 при high_inflation, нейтрален при низкой

## Почему важно
Репортировать "r = 0.5 по всему периоду" без режимного условия — вводит в заблуждение.
Настоящий сигнал может быть r=0.8 в одном режиме и r=-0.3 в другом.

## Связанные атаки
- [[attack_regime_conditionality]] — задокументированная методологическая уязвимость
- [[attack_non_stationarity]] — нестационарность как следствие смены режимов
