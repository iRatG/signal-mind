---
type: decision
status: active
decision_date: 2026-04-20
alternatives_considered: [FAISS, Weaviate, Pinecone]
tags: [architecture, rag, vector_db]
updated: 2026-05-02
---

# Decision: ChromaDB для RAG

## Что решили
ChromaDB — векторная БД для трёх коллекций:
- `regulatory_docs` — ЦБ документы (17 492 чанков)
- `corp_reports` — корпоративные отчёты
- `methodology` — Obsidian vault (новая, Петля 4)

## Почему ChromaDB
- Локальный деплой, файл в `db/chroma/`
- Простой Python API
- Upsert по ID — удобно для обновления vault без дублей
- Персистентный клиент — не теряет данные между запусками

## Модель эмбеддингов
`paraphrase-multilingual-MiniLM-L12-v2` — поддерживает русский язык, быстрый.

## Параметры для methodology коллекции
- Chunk size: 600 токенов (секции H2 в markdown)
- Overlap: 100 токенов
- ID чанка: `{file_stem}__{section_slug}` — стабильный для upsert
