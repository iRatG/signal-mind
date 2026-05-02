---
type: decision
status: active
decision_date: 2026-04-15
alternatives_considered: [GPT-4, Claude, Llama-local]
tags: [architecture, llm]
updated: 2026-05-02
---

# Decision: DeepSeek API как основной LLM

## Что решили
Модель: `deepseek-chat`. Переменная: `deep_seek_token`.

## Почему DeepSeek
- Стоимость: ~$0.003–0.005 на итерацию (vs $0.02+ у GPT-4)
- За 3416 итераций: ~$10 total
- Качество генерации SQL гипотез: достаточное
- API стабильный, latency приемлемый

## Ограничения
- Не самая сильная модель для сложного reasoning
- Confirmation bias (см. [[approach_llm_self_eval]] deprecated)
- Иногда повторяющиеся паттерны без явной инструкции

## Альтернативы (рассматривались)
- GPT-4o: лучше, но дороже в 5x
- Claude Sonnet: хорош, но дороже
- Llama local: бесплатно, но требует GPU и медленнее
