---
type: approach
status: deprecated
marathon_introduced: 1
marathon_deprecated: 2
replaced_by: approach_deterministic_holdout
tags: [methodology, evaluation, deprecated]
updated: 2026-05-02
---

# Approach: LLM Self-Evaluation — DEPRECATED ⚠️

## ВАЖНО: ЭТОТ ПОДХОД ОТБРОШЕН

DeepSeek сам оценивал свои гипотезы → inflation confirmed rate ~3x.
**Не использовать. Заменён детерминистической верификацией.**

## Суть (была)
DeepSeek получал SQL-результат и сам ставил `confirmed: true/false` и `signal_score: 0-100`.

## Почему взяли (Marathon 1)
- Быстро и дёшево
- confirmed rate 67.7% выглядел правдоподобно

## Что обнаружили (Phase 6 аудит)
- LLM confirmed rate: **66.4%**
- Real confirmed rate: **~17–20%**
- Разрыв: **3x inflation**

Причины инфляции:
1. Instrument aliasing: CORR(IMOEX, IMOEX) = 0.88 → агент считал "confirmed"
2. Tautological correlation: EUR/RUB vs USD/RUB → r=0.97 по построению
3. Confirmation bias: агент хотел найти сигнал

## Масштаб ущерба
- Marathon 1–2: ~60% "confirmed" были артефактами
- experiments.db за эти периоды непригоден для fine-tuning без чистки

## Чем заменили
Детерминистические проверки Revizor (6 типов):
ALIASING, TAUTOLOGY, REGIME_INACTIVE, STRUCTURAL, WEAK_R, CONVERGENCE

## Урок
**Никогда не доверять self-reported метрикам без независимой верификации.**
Даже большая выборка не защищает от системного смещения.

## Связанные атаки
- [[attack_llm_eval_inflation]] — задокументированная атака этого подхода
