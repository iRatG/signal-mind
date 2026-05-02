---
type: attack
status: open
target_signals: [signal_usd_rub_moexfn, signal_sanctions_usd_rub, signal_inflation_moex10]
severity: major
closes_with: regime_split_analysis
tags: [methodology, regime, conditionality]
updated: 2026-05-02
---

# Attack: Regime Conditionality

## Суть атаки
Многие сигналы работают только в определённом рыночном режиме.
Средний r по всему периоду маскирует сильные r в одном режиме и нулевые/негативные в другом.

## Примеры
- USD/RUB → MOEXFN: работает при ставке 7–15%, ломается при 21%
- Sanctions → USD/RUB: работает при слабом рубле (>80), слабый при сильном
- Inflation → MOEX10: работает при inflation>8%, нейтрален при низкой инфляции

## Режимы системы
```json
{"regime": "rate_hike", "key_rate": ">= 15%"}
{"regime": "high_inflation", "inflation": ">= 8%"}
{"regime": "weak_ruble", "usd_rub": ">= 80"}
{"regime": "bear_market", "imoex_trend": "< -10% за 3м"}
```

## Что нужно для закрытия
Для каждого сигнала: провести анализ r в каждом режиме отдельно.
Если r в одном режиме >> среднего → сигнал условный, документировать условие.

## Статус: **OPEN — P1**

## Связанные концепты
- [[concept_regime_conditionality]]
