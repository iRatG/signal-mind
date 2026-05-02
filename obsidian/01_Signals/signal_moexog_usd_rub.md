---
type: signal
status: partial
instrument_a: MOEXOG
instrument_b: USD_RUB
lag_days: 0
r: 0.70
n: 900
confidence: medium
marathon_found: 2
tags: [signal, oil_gas, forex]
updated: 2026-05-02
---

# Signal: MOEXOG → USD/RUB (синхронный)

## Summary
Нефтяной сектор MOEX сильно коррелирует с курсом доллара.
r = +0.70, синхронная корреляция. Механизм: нефть → рублёвая выручка → курс.
**Статус: partial** — направление причинности неясно (reverse causality возможна).

## Статистика
- r: +0.70
- lag: 0 дней (синхронно)
- n: ~900
- Период: 2022–2025

## Экономический механизм
Нефтяные компании → рублёвая выручка от экспорта нефти → репатриация валюты →
укрепление рубля когда нефть растёт (обратная связь).

**Reverse causality:** слабый рубль → рублёвая стоимость нефтяного экспорта растёт
→ MOEXOG растёт (оценка нефтяников в рублях).

## Открытые вопросы
- Что первично: MOEXOG → USD/RUB или USD/RUB → MOEXOG?
- Granger causality тест нужен для установления направления
- Lag анализ: есть ли lead/lag structure (MOEXOG опережает USD/RUB на N дней)?

## Связанные сигналы
- [[signal_usd_rub_moexfn]] — через рубль связано с банковским сектором
- [[signal_brent_moexfn]] — нефть как общий фактор

## Связанные концепты
- [[concept_regime_conditionality]]
