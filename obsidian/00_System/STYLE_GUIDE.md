---
type: system
updated: 2026-05-02
---

# Style Guide — Signal Mind Vault

## Frontmatter стандарт

```yaml
---
type: signal | approach | attack | decision | concept | moc | source
status: confirmed | partial | not_tested | deprecated | invalidated | open | closed | active
tags: []
updated: YYYY-MM-DD
---
```

### Дополнительные поля по типу

**signal:**
```yaml
instrument_a: USD_RUB
instrument_b: MOEXFN
lag_days: 14
r: 0.758
n: 990
confidence: high | medium | low
marathon_found: 1
```

**attack:**
```yaml
target_signals: []
severity: critical | major | minor
closes_with: ""
```

**approach:**
```yaml
marathon_introduced: 1
marathon_deprecated: null
replaced_by: ""
```

## Именование файлов

| Тип | Паттерн | Пример |
|-----|---------|--------|
| signal | signal_{a}_{b}.md | signal_usd_rub_moexfn.md |
| approach | approach_{name}.md | approach_ouroboros_loop.md |
| attack | attack_{name}.md | attack_non_stationarity.md |
| decision | decision_{topic}.md | decision_duckdb.md |
| concept | concept_{name}.md | concept_regime_conditionality.md |
| map | MOC_{topic}.md | MOC_all_instruments.md |

## Wikilinks

Использовать `[[filename_without_extension]]` для связей.
Каждая страница signal должна ссылаться на связанные attacks и concepts.
Каждая атака должна ссылаться на signal которые она атакует.

## Статусы сигналов

| Статус | Значение |
|--------|----------|
| confirmed | r ≥ 0.50, n ≥ 200, проверен на нескольких периодах |
| partial | r ≥ 0.30, но нестабилен или мало данных |
| not_tested | инструмент/пара не проверялась |
| under_attack | confirmed, но есть открытые методологические атаки |
| invalidated | была гипотеза, отклонена данными |
| deprecated | подход заменён новым |
