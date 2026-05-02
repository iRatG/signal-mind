---
type: signal
status: under_attack
instrument_a: MSCI_INDIA
instrument_b: MOEXFN
lag_days: 0
r: 0.65
n: 800
confidence: low
marathon_found: 2
tags: [signal, emerging_markets, banking, moexfn]
updated: 2026-05-02
---

# Signal: MSCI India → MOEXFN (lag 0d)

## Summary
Синхронная корреляция между индексом MSCI India и российским финансовым индексом.
r ≈ +0.65, lag = 0 дней. Гипотеза: оба реагируют на глобальный emerging market аппетит к риску.
**Статус: под атакой** — механизм слабо обоснован, возможна ложная корреляция через DXY.

## Статистика
- r: ~0.65
- lag: 0 дней
- n: ~800
- Период: 2022–2025
- Marathon found: 2

## Экономический механизм (гипотетический)
Глобальный риск-аппетит → инвесторы покупают EM активы (India + Russia) →
оба индекса растут синхронно.

**Альтернативная гипотеза (spurious):** оба коррелируют с DXY (ослабление доллара →
рост всех EM активов) → корреляция не является прямой причинно-следственной.

## Открытые атаки
- [[attack_in_sample_overfitting]] — нет holdout ← **P0**
- [[attack_non_stationarity]] — стабильность не проверена ← **P0**
- [[attack_regime_conditionality]] — работает ли при закрытых рынках (2022 март)?
- Подозрение: spurious через DXY — нужно partial correlation с контролем DXY

## Связанные концепты
- [[concept_regime_conditionality]]
- [[concept_signal_nonstationarity]]

## Связанные сигналы
- [[signal_usd_rub_moexfn]] — возможно оба через USD/RUB канал
- TODO: [[signal_msci_world_imoex]] — более широкий EM сигнал?

## Open questions
- Partial correlation MSCI_INDIA → MOEXFN при контроле DXY — остаётся ли r > 0.4?
- Проверить MSCI_WORLD → MOEXFN — более прямая связь?
- Проверить DJ_SOUTH_AFRICA, CHINA_H_SHARES → другие EM корреляции
