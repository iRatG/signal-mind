# Ticket 05 - Source Strategy

Status: open
Type: research
Labels: `wayfinder:research`
Claim: unclaimed
Blocks: Ticket 06 - Evaluation Harness, Ticket 08 - OpenSpec Bridge

## Question

Which sources should the MVP trust, compare, exclude, or keep as disabled candidates?

## Why This Matters

The current active set is Kommersant, Interfax, Lenta, Vedomosti, and RIA. RBC is disabled because of Qrator blocking; TASS returned 403 from the server. Source mix directly shapes the radar's view of agenda pressure.

## Research Scope

Investigate:

- whether RBC has a stable RSS/API/archive route that avoids brittle scraping;
- whether TASS has a legitimate accessible route;
- whether additional sources should cover business, state media, opposition/independent media, regional signals, or international wires;
- what each source contributes and what bias/noise it adds;
- whether source categories should become part of scoring.

## Decision Shape

Produce a source policy:

- active sources for MVP;
- disabled candidates with reasons;
- source categories;
- available text depth by source: headline only, headline plus description/lead, or full article candidate;
- minimum source diversity for each signal type;
- future source backlog.

## Forward Note (2026-07-31, captured while grilling Ticket 02 - Signal Ontology, not resolved)

Airat wants the news-ingestion layer treated as a pluggable module behind a stable interface — today it's a handful of Russian news-site parsers, but the interface should be able to add RSS feeds, Telegram channels, or third-party datasets (e.g. Kaggle) without changing downstream analysis. He mentioned we may already have a ~6-year US news-background dataset somewhere and offered to locate it precisely later. This connects directly to the map's existing "Not yet specified" item — "whether to keep Russian-only scope or add foreign news sources for geopolitical context" — resolve both together when this ticket is claimed rather than deciding the interface shape without also deciding scope.

Second forward note (2026-07-31, captured while grilling One-source anomaly on Ticket 02): Airat wants source categorization — state-aligned (РБК/Коммерсант/Ведомости), independent/opposition (e.g. Радио Свобода), foreign (tagged by country) — to feed the One-source anomaly vs Noise/routine split. His reasoning: a story reported only by an independent/opposition outlet, with state-aligned outlets silent, may reflect selective non-coverage rather than the story being unreal or unimportant — source count alone shouldn't downgrade it. This is exactly the "state media, opposition/independent media... what bias/noise it adds... whether source categories should become part of scoring" work already in this ticket's Research Scope — Ticket 02 only records the principle (source category is a modifier on top of the follow-up/market-corroboration check, not a replacement for it); the actual category definitions and scoring weight belong here.

## Forward Note (2026-07-31, dataset located)

The ~6-year US news-background dataset mentioned above is `db/hf_news.db` (SQLite, READ-ONLY per `CLAUDE.md`, not present in this checkout). Corrected facts, verified against the loader code rather than assumed:

- It is overwhelmingly a single English-language financial-news corpus (`Brianferrell787/financial-news-multisource` via `src/parsers/hf_news_loader.py`), filtered to `date >= 2021-01-01`, ~2.52M articles, ~9.93 GB (`analytics/SESSION_LOG.md`). Actual content stops around 2025-09-06 (`GAP_START` in `src/parsers/gdelt_loader.py`), so the usable full-text window is ~4.5 years (2021-01 to 2025-09), not 6.
- It is **not** a matched multi-year Russian corpus. The only Russian content is `ru_archive:`-prefixed rows from `ru_news_archive_loader.py`, a small backfill (order ~28k articles per `src/agent/news_retriever.py`) covering only the post-gap tail (~2025-09 onward), from the same 5 RU sites the live collector already uses.
- Coverage past 2025-09-06 is filled separately by `db/news_gdelt.db` (English, pre-aggregated daily topic counts only, no article text); there is no equivalent pre-aggregated post-gap source for Russian beyond the live collector itself.
- `db/hf_news.db` is also the source for a separate one-off study, [Ticket 12 - Historical Cluster Calibration Study](12-historical-cluster-calibration.md) — that ticket owns deriving cluster-size/persistence/source-spread numbers from it. This ticket's own scope (source policy, categories, text-depth-by-source) is unaffected and does not need to reopen just because the dataset is now located.
