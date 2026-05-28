# Hand-off — Signal Mind

**Последнее обновление:** 2026-05-28 22:00  
**Ветка:** master  
**Последний коммит:** c9cad53

---

## Статус системы прямо сейчас

### Task Scheduler — 4 задачи

| Задача | Время | Последний запуск | Статус |
|---|---|---|---|
| SignalMind-NightSearch | 06:00 + 22:00 | 28.05 22:00 | 🟢 работает сейчас |
| SignalMind-Analyze | 13:00 | 28.05 13:00 | ✅ код 0 |
| SignalMind-Afternoon | 14:00 | 28.05 14:00 | ✅ исправлен (em-dash) |

### Скрипты

| Скрипт | Назначение | Статус |
|---|---|---|
| `scripts/night_run.ps1` | 06:00 + 22:00 | ✅ работает |
| `scripts/afternoon_run.ps1` | 14:00 | ✅ исправлен (ASCII only, no em-dash) |
| `scripts/analyze_and_update.ps1` | 13:00 | ✅ работает |
| `scripts/morning_check.ps1` | утренняя проверка | ✅ |

---

## Итоги Phase B (9 сессий, 2026-05-25 → 2026-05-28)

### Ключевые цифры
- Сессий: **9**
- Train сигналов накоплено: **68 428**
- Val-confirmed накоплено: **21 489**
- Val pass rate: **31.4%** (здоровый)
- IC trend: **stable**

### Топ стабильные сигналы (появляются в 2+ сессиях)

| Сигнал | IC (лучший) | M5 p | Сессий |
|---|---|---|---|
| SP500 / inflation_z / 7d | 0.239 | 0.017 | 3+ |
| MOEXOG / sanctions_z / 7d | 0.189 | 0.043 | 3+ |
| GOLD / inflation_z / 7d | 0.138 | 0.016 | 3+ |
| MOEXFN / sanctions_z / 1d | 0.143 | 0.022 | 3+ |
| DXY / sanctions_z / 14d | 0.144 | 0.004 | 2+ |
| MOEXFN / rate_emb_z / 14d | 0.177 | 0.128 | 2+ |

### session_config.json (последний от 13:00 28.05)
```json
{
  "ic_gate": 0.05,
  "ensemble": "M5_AND_M6",
  "best_feature": "embedding_z90",
  "best_lags": [2, 8, 23, 60],
  "top_instruments": ["GOLD", "MSCI_INDIA", "MSCI_WORLD", "SP500", "MOEXOG", "CHINA_H_SHARES", "FTSE_CHINA_50", "BRENT"],
  "top_topics": ["oil_emb_z", "inflation_z", "rate_emb_z", "oil_emb", "gold_emb_z"],
  "stable_signals": 10,
  "val_pass_rate": 0.314
}
```

---

## Phase C — готова к реализации

**Документация:** `analytics/phase_c/DESIGN.md`  
**Код:** `src/pipeline_c/__init__.py` (заглушка)

### Что строить в следующей сессии

```
src/pipeline_c/
  hypothesis_schema.py    ← ПЕРВЫЙ ШАГ
  rag_extractor.py        ← ВТОРОЙ ШАГ
  hypothesis_tester.py    ← ТРЕТИЙ ШАГ
  phase_c_runner.py       ← ЧЕТВЁРТЫЙ ШАГ
```

**Данные готовы:**
- Corp reports: 15 574 чанков (Sberbank 4614, Lukoil 3770, Gazprom 3670, Yandex 3115)
- Regulatory: 1 918 чанков ЦБ РФ
- Obsidian vault: 221 чанк (signal/attack/approach/concept)
- `src/agent/rag.py` — уже работает, не надо писать заново

**Принцип Phase C:**
1. RAG (ChromaDB) → LLM (DeepSeek) → структурированные гипотезы в JSON файлы
2. Человек смотрит файлы и при необходимости корректирует
3. M5+M6 тест с z-score и правильными сплитами
4. Integrity check + ledger
5. Obsidian vault обновляется результатами

---

## Habr статья — готова к публикации

**HTML:** `habr/signal_mind_phase2.html` — открывается в браузере, все графики встроены  
**MD:** `habr/phase_b_article.md` — markdown версия

**Структура статьи:**
- Часть 1: предметная область (нормализация, сплиты, 2 production сигнала, режимный сдвиг)
- Часть 2: работа с LLM (5 правил, трёхуровневая память, integrity check, 3 бага)
- Phase C preview

**Графики в статье:**
1. IC до/после z-score нормализации
2. Train/Val/Test timeline
3. Воронка сигналов (5000 → 812 → 54 → 2)
4. Знаковые флипы (57%)
5. Obsidian vault распределение
6. Enrichment по сессиям

---

## Исправленные баги (важно помнить)

| Баг | Файл | Причина | Исправление |
|---|---|---|---|
| Infinite loop Ouroboros | night_search.py | метки с round_num | content-based метки |
| NameError double_confirmed | night_search.py | рефакторинг | переименовал |
| Analyzer AttributeError | inter_session_analyzer.py | pandas groupby.apply | axis=1 lambda |
| afternoon em-dash | afternoon_run.ps1 | кодировка UTF-8 vs CP1251 | ASCII only |
| afternoon backtick | afternoon_run.ps1 | Task Scheduler | одна строка |

---

## Правила (не нарушать)

- `db/hf_news.db` — только читать, никогда не удалять
- `db/signal_mind.duckdb` — только читать или append
- `db/emb_cache/` — не удалять (8 часов пересчёта)
- `analytics/testbed/` — frozen
- `habr/sample/` — frozen (опубликованные ссылки)
- Phase A и Phase B не трогать — они работают
