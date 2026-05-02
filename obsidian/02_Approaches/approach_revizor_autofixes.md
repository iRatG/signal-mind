---
type: approach
status: active
marathon_introduced: 3
tags: [methodology, audit, quality]
updated: 2026-05-02
---

# Approach: Revizor — независимый аудитор

## Суть
После каждого марафона Revizor запускает 6 детерминистических проверок на всех confirmed сигналах.
Независим от агента — не подвержен confirmation bias.

## 6 проверок
1. **ALIASING** — одна колонка в обоих аргументах CORR()
2. **TAUTOLOGY** — тавтологичная пара (EUR/RUB vs USD/RUB → r=0.97)
3. **REGIME_INACTIVE** — сигнал не работает в текущем режиме
4. **STRUCTURAL** — r меняется >0.08 между первой и второй половиной периода
5. **WEAK_R** — r < MIN_R (0.30) — слишком слабый
6. **CONVERGENCE** — топик повторяется > N раз → автоматический blacklist

## Результат
- Real confirmed rate после Revizor: ~15–25% (vs 66% до)
- Revizor saved: предотвратил накопление ложных паттернов в experiments.db

## Файл
`src/agent/revizor.py`

## Связанные атаки
- [[attack_llm_eval_inflation]] — закрыта этим подходом ✅
