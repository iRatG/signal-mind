# Signal Mind — Scientific Ledger
Generated: 2026-05-29 01:55 UTC  |  Total records: 9

---

## ОБЩАЯ ЧЕСТНОСТЬ СИСТЕМЫ

| Метрика | Значение | Интерпретация |
|---|---|---|
| Всего сессий | 6 | — |
| Гипотез проверено | 586,544 | все за всё время |
| Ожидается случайных (p<0.05) | 29327 | 586544×0.05 |
| Train сигналов найдено | 25088 | реально |
| Обогащение vs случай | **0.9x** | ❌ шум |
| Val-confirmed | 12979 | прошли holdout |
| Val pass rate | 51.7% | 12979/25088 |
| Среднее обогащение/сессию | 0.71x | — |
| Сессий с реальным сигналом | 0/6 | вердикт SIGNAL |
| Проблемных сессий | 6/6 | NOISE или CONCERN |

## ИСТОРИЯ СЕССИЙ

| Сессия | Тип | Гипотез | Train | Val | Обогащение | Вердикт | FAIL флаги |
|---|---|---|---|---|---|---|---|
| 20260525_190003 | ? | 115584 | 4778 | 2595 | 0.83x | NOISE — results indistinguishable f | 🟡 |
| 20260527_030004 | ? | 120848 | 5242 | 2596 | 0.87x | NOISE — results indistinguishable f | 🟡 |
| 20260527_190605 | ? | 0 | 0 | 0 | 0.00x | NOISE — results indistinguishable f | ✅ |
| 20260527_190005 | ? | 115584 | 5019 | 2596 | 0.87x | NOISE — results indistinguishable f | 🟡 |
| 20260528_030004 | ? | 119392 | 5037 | 2596 | 0.84x | NOISE — results indistinguishable f | 🟡 |
| 20260528_190004 | ? | 115136 | 5012 | 2596 | 0.87x | NOISE — results indistinguishable f | 🟡 |

## КРИТИЧЕСКИЕ ПРОБЛЕМЫ (FAIL)

✅ Критических проблем не зафиксировано

## ПРЕДУПРЕЖДЕНИЯ (WARN)

- 🟡 **ENRICHMENT_NEAR_CHANCE** (×5) — Train signals (5012) barely above random expectation (5756.8 at p<0.05). Enrichment=0.87x — could be noise.

## ИСТОРИЯ ИЗМЕНЕНИЙ КОНФИГА

*(конфиг не менялся)*

## УСТОЙЧИВЫЕ СИГНАЛЫ (ТОП)

*(нет данных)*

## АНТИ-ПАТТЕРНЫ (вечные неудачники)


---
*Ledger path: C:\project\signal_mind\analytics\phase_b\ledger.jsonl*
