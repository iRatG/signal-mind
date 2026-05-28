# Signal Mind — Scientific Ledger
Generated: 2026-05-28 10:00 UTC  |  Total records: 8

---

## ОБЩАЯ ЧЕСТНОСТЬ СИСТЕМЫ

| Метрика | Значение | Интерпретация |
|---|---|---|
| Всего сессий | 5 | — |
| Гипотез проверено | 471,408 | все за всё время |
| Ожидается случайных (p<0.05) | 23570 | 471408×0.05 |
| Train сигналов найдено | 20076 | реально |
| Обогащение vs случай | **0.9x** | ❌ шум |
| Val-confirmed | 10383 | прошли holdout |
| Val pass rate | 51.7% | 10383/20076 |
| Среднее обогащение/сессию | 0.68x | — |
| Сессий с реальным сигналом | 0/5 | вердикт SIGNAL |
| Проблемных сессий | 5/5 | NOISE или CONCERN |

## ИСТОРИЯ СЕССИЙ

| Сессия | Тип | Гипотез | Train | Val | Обогащение | Вердикт | FAIL флаги |
|---|---|---|---|---|---|---|---|
| 20260525_190003 | ? | 115584 | 4778 | 2595 | 0.83x | NOISE — results indistinguishable f | 🟡 |
| 20260527_030004 | ? | 120848 | 5242 | 2596 | 0.87x | NOISE — results indistinguishable f | 🟡 |
| 20260527_190605 | ? | 0 | 0 | 0 | 0.00x | NOISE — results indistinguishable f | ✅ |
| 20260527_190005 | ? | 115584 | 5019 | 2596 | 0.87x | NOISE — results indistinguishable f | 🟡 |
| 20260528_030004 | ? | 119392 | 5037 | 2596 | 0.84x | NOISE — results indistinguishable f | 🟡 |

## КРИТИЧЕСКИЕ ПРОБЛЕМЫ (FAIL)

✅ Критических проблем не зафиксировано

## ПРЕДУПРЕЖДЕНИЯ (WARN)

- 🟡 **ENRICHMENT_NEAR_CHANCE** (×4) — Train signals (5037) barely above random expectation (5969.6 at p<0.05). Enrichment=0.84x — could be noise.

## ИСТОРИЯ ИЗМЕНЕНИЙ КОНФИГА

*(конфиг не менялся)*

## УСТОЙЧИВЫЕ СИГНАЛЫ (ТОП)

*(нет данных)*

## АНТИ-ПАТТЕРНЫ (вечные неудачники)


---
*Ledger path: C:\project\signal_mind\analytics\phase_b\ledger.jsonl*
