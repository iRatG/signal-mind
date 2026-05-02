---
type: attack
status: closed
target_approaches: [approach_llm_self_eval]
severity: critical
closed_by: approach_revizor_autofixes
closed_date: 2026-04-30
tags: [methodology, evaluation, closed]
updated: 2026-05-02
---

# Attack: LLM Evaluation Inflation — ЗАКРЫТА ✅

## Суть атаки (была)
DeepSeek сам оценивал свои гипотезы → confirmed rate 66.4% vs реальные ~17–20%.
Разрыв: **3x inflation**.

## Причины (установлены)
1. **Instrument aliasing** (60% ложных) — CORR(col, col) = 0.88
2. **Tautological correlation** — EUR/RUB vs USD/RUB r=0.97
3. **Confirmation bias** — агент "хотел" найти сигнал

## Как закрыли
Revizor (6 детерминистических проверок) + aliasing detector.
После внедрения: реальный confirmed rate ~15–25%.

## Урок
Self-reported метрики без независимой верификации — опасная иллюзия.
Размер выборки не защищает от системного смещения.

## Связанные файлы
- [[approach_llm_self_eval]] — deprecated подход
- [[approach_revizor_autofixes]] — решение
