---
type: system
updated: 2026-05-02
---

# Agent Roles — Signal Mind Vault

## Role 1: Librarian (Ingestion)

Запускается после каждой итерации агента через wiki_writer.py.

**Входные данные:** исход итерации (confirmed/rejected), сигнал (инструмент, лаг, r, n), режим рынка.

**Действия:**
- Если confirmed (r ≥ 0.50): обновить или создать 01_Signals/{signal_id}.md
- Если новый анти-паттерн: обновить 03_Attacks/
- Если хороший SQL найден: обновить 03_Wiki/methods/
- Всегда: записать в 02_Sources/experiments/{date}.md

**Правило минимального шума:** не создавать много страниц сразу. Сначала source note, потом canonical page.

---

## Role 2: Synthesizer (Knowledge Building)

Запускается после каждого марафона через revizor.py.

**Действия:**
- Обновить canonical pages для сигналов с новыми данными r/n
- Создать 05_Workbench/synthesis/marathon_N_synthesis.md
- Обновить MOC файлы в 06_Maps/
- Обновить concept_full_data_scope.md — пометить что протестировано

---

## Role 3: Researcher (Deep Dive)

Запускается при необходимости понять "что мы знаем по теме X".

**Рабочий цикл:**
1. Уточнить исследовательский вопрос
2. Найти релевантные файлы (signals, concepts, attacks)
3. Построить карту аргументов: известно / спорно / не хватает
4. Вернуть краткий brief

---

## Role 4: Editor (Quality Maintenance)

Запускается вручную или раз в неделю.

**Проверяет:**
- Дубли concept/entity pages
- Страницы без summary
- Битые wikilinks
- Устаревшие статусы (signal изменил r?)

---

## Role 5: Planner (Strategy)

Запускается вручную для планирования следующего марафона.

**Задача:** взять vault как контекст → спланировать:
- Какие инструменты/индексы ещё не тестировались (из concept_full_data_scope.md)
- Какие лаги не покрыты
- Где база знаний поверхностна

**Приоритет плана:** всегда начинать с нетестированных областей.
