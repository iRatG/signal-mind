---
type: system
updated: 2026-05-02
---

# Vault Index — Signal Mind

## Навигация

- [[concept_full_data_scope]] ← ПОЛНЫЙ СКОУП: все 200+ пар инструментов со статусом тестирования
- [[MOC_all_instruments]] ← карта всех 16 индексов и 13 инструментов
- [[MOC_all_signals]] ← карта всех найденных сигналов

## 01_Signals — Сигналы

| Файл | r | Статус |
|------|---|--------|
| [[signal_usd_rub_moexfn]] | 0.758 | under_attack |
| [[signal_brent_moexfn]] | 0.71 | under_attack |
| [[signal_msci_india_moexfn]] | ~0.65 | under_attack |
| [[signal_moexog_usd_rub]] | 0.70 | partial |
| [[signal_oil_news_moexog]] | 0.11–0.16 | partial |
| [[signal_sanctions_usd_rub]] | 0.43 | partial |
| [[signal_inflation_moex10]] | -0.34 | partial |

## 02_Approaches — Подходы

| Файл | Статус |
|------|--------|
| [[approach_ouroboros_loop]] | active |
| [[approach_llm_self_eval]] | **deprecated** |
| [[approach_aliasing_detection]] | active |
| [[approach_revizor_autofixes]] | active |
| [[approach_anti_convergence]] | active |
| [[approach_full_scope_sweep]] | active |

## 03_Attacks — Открытые атаки

| Файл | Severity | Статус |
|------|----------|--------|
| [[attack_in_sample_overfitting]] | critical | OPEN |
| [[attack_non_stationarity]] | critical | OPEN |
| [[attack_multiple_testing]] | major | OPEN |
| [[attack_llm_eval_inflation]] | critical | closed |
| [[attack_regime_conditionality]] | major | OPEN |
| [[attack_scope_convergence]] | major | OPEN |

## 04_Decisions — Архитектурные решения

- [[decision_duckdb]]
- [[decision_deepseek_api]]
- [[decision_chromadb_rag]]

## 05_Concepts — Концепты

- [[concept_full_data_scope]] ← КЛЮЧЕВОЙ ФАЙЛ
- [[concept_regime_conditionality]]
- [[concept_signal_nonstationarity]]
- [[concept_weak_signal_detection]]
- [[concept_self_repair_loop]]
- [[concept_lag_hypothesis]]

## 06_Maps

- [[MOC_all_instruments]]
- [[MOC_all_signals]]
