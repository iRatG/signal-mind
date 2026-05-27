# Hand-off — Signal Mind

**Последнее обновление:** 2026-05-27  
**Статус:** Phase B работает автономно. Phase C спроектирована, готова к реализации.

---

## Прочитай это первым

Мы прошли два полных цикла и начинаем третий.

**Phase A** (src/agent/): RAG + LLM + Ouroboros. Хорошая генерация гипотез, плохая валидация.  
**Phase B** (src/pipeline_v2/): M5+M6, z-score, правильные сплиты. Хорошая валидация, нет интеллекта.  
**Phase C** (src/pipeline_c/): объединяем лучшее. Документация: `analytics/phase_c/DESIGN.md`.

---

## Phase B — автономный статус

Работает по расписанию без участия человека:

| Время | Задача | Статус |
|---|---|---|
| 06:00 | night_search --loop-hours 7 | ✅ работает |
| 13:00 | inter_session_analyzer | ✅ работает |
| 14:00 | afternoon_run --loop-hours 7 | ✅ исправлен (был баг с бэктиком) |
| 22:00 | night_search --loop-hours 7 | ✅ работает |

Утренняя проверка: `.\scripts\morning_check.ps1`

**Исправленные баги Phase B:**
- Ouroboros infinite loop (метки с round_num → content-based метки)
- NameError double_confirmed → double_confirmed_keys
- Analyzer AttributeError (pandas groupby.apply)
- afternoon_run бэктик-перенос (Task Scheduler)

---

## Phase C — что делать в следующей сессии

### Шаг 1: hypothesis_schema.py
```python
@dataclass
class RagHypothesis:
    instrument: str          # MOEXFN, MOEXOG, IMOEX, ...
    feature: str             # rate_z, oil_z, sanctions_z, ...
    lag_range: list[int]     # [7, 14, 30] — из текста отчёта
    direction: str           # "positive" | "negative" | "unknown"
    rationale: str           # цитата или парафраз из отчёта
    source_company: str      # sberbank | lukoil | gazprom | yandex
    source_year: int         # 2022 | 2023 | 2024
    source_page: int         # номер страницы
    confidence: float        # 0-1, оценка LLM
    hypothesis_id: str       # uuid
```

### Шаг 2: rag_extractor.py
Для каждой пары (company, year):
1. Запрос к ChromaDB: "чувствительность к рынку", "факторы риска"
2. LLM читает 5-8 чанков → генерирует список RagHypothesis
3. Сохраняет в `analytics/phase_c/hypotheses/{company}_{year}.json`

Компании: sberbank, lukoil, gazprom, yandex  
Годы: 2021, 2022, 2023, 2024  
= ~16 файлов гипотез

### Шаг 3: hypothesis_tester.py
- Принимает RagHypothesis
- Тестирует все лаги из lag_range + соседние
- M5+M6 на Train split (2022-01-01 → 2023-09-30)
- Val holdout (2024-01-01 → 2025-04-30)
- z-score нормализация (обязательно)
- Возвращает: TestResult с IC, p-value, confirmed, цитата

### Шаг 4: phase_c_runner.py
Оркестратор ночного запуска Phase C.

---

## Данные Phase C (READ-ONLY)

```python
# ChromaDB — гипотезы
from src.agent.rag import search_corp, search_regulatory

# Market data — тестирование
from src.pipeline_v2.big_scanner import load_market_split, MARKET_INSTRUMENTS
from src.pipeline_v2.feature_transformer import FeatureTransformer

# M5+M6 — валидация  
# analytics/testbed/methods/m5_var.py
# analytics/testbed/methods/m6_lgbm.py

# Сплиты
# Train: 2022-01-01 → 2023-09-30
# Val:   2024-01-01 → 2025-04-30
# Test:  2025-09-01 → 2026-04-29 (ИСПОЛЬЗОВАН В PHASE B — не трогать)
```

---

## Правила (не нарушать)

- `db/hf_news.db` — только читать
- `db/signal_mind.duckdb` — только читать или append
- `db/emb_cache/` — не удалять (8 часов пересчёта)
- `analytics/testbed/` — frozen
- Phase A и Phase B не трогать — они работают

---

## Итоги Phase B (для справки, не как семена)

За 7+ сессий:
- 66 831 хит, 6 сессий
- Val pass rate: 25.8%
- IC trend: improving
- Лучшие конфиги: embedding_z90, M5_AND_M6, IC_gate=0.05
- Топ инструменты: MSCI_INDIA, GOLD, MSCI_WORLD, CHINA_H_SHARES

Эти данные — для контекста. Phase C ищет новое, не подтверждает найденное.
