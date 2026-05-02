---
type: signal
status: partial
instrument_a: news_inflation
instrument_b: MOEX10
lag_days: 14
r: -0.34
n: 800
confidence: low
marathon_found: 1
tags: [signal, news, inflation, bluechip]
updated: 2026-05-02
---

# Signal: Inflation news → MOEX10 downside (lag 14d)

## Summary
Новостной топик "инфляция" предсказывает снижение голубых фишек через 14 дней.
r = -0.34 в режиме HIGH inflation. Сигнал инверсный — больше новостей об инфляции = рынок падает.
**Статус: partial** — слабый r, режимная зависимость.

## Статистика
- r: -0.34 (в HIGH inflation режиме)
- lag: 14 дней
- n: ~800 (режимная подвыборка)
- Условие: инфляция > 8%

## Экономический механизм
Рост инфляционных ожиданий → ЦБ повысит ставку → рост стоимости долга →
компании MOEX10 снижают прибыль → котировки падают.

## Open questions
- Проверить без режимного условия — исчезает ли сигнал?
- Проверить news_rate × news_inflation взаимодействие
- Проверить на других индексах: MOEXFN, MOEXOG реагируют по-другому?

## Связанные атаки
- [[attack_regime_conditionality]]
- [[attack_multiple_testing]]

## Связанные сигналы
- [[signal_usd_rub_moexfn]] — инфляция → ставка → банки
