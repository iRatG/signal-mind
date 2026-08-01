# Ticket 12 — Historical Cluster Calibration Study: Findings

Run date: 2026-08-01. Source: `db/hf_news.db` (read-only, never modified). Algorithm-fit decision:
**(b)** — headline-equivalent field derived from `text`/`extra_fields.title`, clustered with the
production Union-Find/jaccard algorithm (`analytics/news_pressure_cluster.py` defaults:
`min_shared_tokens=2`, `jaccard_threshold=0.24`, `max_token_df=320`, `min_cluster_size=5`).
Script: `analytics/hf_news_calibration.py`. Backing data: `docs/wayfinder/news-pressure-radar/calibration/progress.db`
(`clusters` table — one row per cluster; `progress_log` table — one row per month processed).

Two passes, reported separately per the ticket's Starting Assumption (not blended):

- **English** — `source='data'`, 2021-01 → 2025-09 (57 months, 2,520,591 articles, 47,068 clusters).
- **Russian** — `source LIKE 'ru_archive:%'`, 2025-09 → 2026-05 (9 months, 28,285 articles, 267 clusters). **Provisional** — short window, only 3 of the production's 14 sources.

## Corpus corrections (found this session, not previously documented)

- The ticket's own text claimed the English corpus was `Brianferrell787/financial-news-multisource`. **Wrong.** It is an aggregate of **14 distinct public datasets**: `fnspid_news` (56%), `headlines_10sites_2007_2022` (23%), `yahoo_finance_articles` (8%), `nyt_articles_2000_present` (5%), plus 10 smaller ones. No such dataset as "Brianferrell787" exists in the data. Corrected on the ticket itself.
- An undocumented `en_archive:*` prefix exists (6 sources, 7,723 articles, 2025-09-01→2025-11-15 only) — a short test window, excluded from this study as insufficient for a distribution.

## Data-quality caveats — read before using any number below

1. **`fnspid_news` (56% of the English corpus) has no recoverable per-article publisher.** Every one of its 1,416,164 articles carries a URL, but 100% of them resolve to the same single domain (`nasdaq.com`) — a re-syndication front, not the original wire source. `publisher`/`author` fields are empty strings for all of them. **Any English cluster dominated by `fnspid_news` content will show `source_spread=1` by construction, regardless of how many actual newsrooms the story really ran in.** This is a property of the archive, not a bug in this script — there is no field left to fall back to.
2. **`headlines_10sites_2007_2022` (23% of the English corpus) has no title/body separator** — title and summary run together with no delimiter, so the extracted "headline" degrades to a longer, noisier text chunk for this sub-dataset. Flagged per-cluster via `degraded_share` in the backing table; not silently smoothed over.
3. **December 2023 is a one-month data-loading artifact, not a real news spike.** It contains 391,867 articles (7-13× a normal month) — 386,072 of them (98.5%) are `fnspid_news`, landing all at once. Its cluster-size upper tail should be read as "one dataset's bulk-load batch," not organic volume.
4. **The Russian pass only has 3 of the production's 14 sources** (interfax, lenta, kommersant). `source_spread` there is hard-capped at 3 — cannot speak to a 14-source production threshold. Matches the ticket's own Starting Assumption: treat as provisional until the live collector accumulates real history.
5. **Velocity was not calibrated.** Clustering runs independently per calendar month (matching the production algorithm's own windowing), so there is no cross-month time series to compute week-over-week velocity from. Ticket 10's velocity-quartile threshold is not addressed by this study — flagged as a gap, not guessed around.
6. **Ticket 03's 30%/50% exclusion-cap and per-flag severity thresholds were not evaluated.** That requires applying `news_pressure_cluster.py`'s actual `quality_flags()` logic (unsupported-number checks, single-source-persistent, etc.) against this historical set — out of tonight's scope (Algorithm-fit was about clustering, not quality-gate simulation). Recorded as follow-up work, not fabricated.

## Findings — English (2021-01 → 2025-09, n=47,068 clusters)

| Metric | min | mean | p50 | p75 | p90 | p95 | p99 | max |
|---|---|---|---|---|---|---|---|---|
| volume (articles/cluster) | 5 | 22.1 | 8 | 24 | 36 | 45 | 104 | 9,004 |
| persistence (days) | 1 | 4.3 | 3 | 5 | 9 | 14 | 28 | 31 |
| source_spread — **see caveat 1, not reliable for FNSPID-heavy clusters** | 1 | 1.74 | 1 | 2 | 4 | 5 | 9 | 97 |

- Share of clusters with `persistence >= 14d` (Ticket 02's current Persistent-background threshold): **5.3%**.
- Share with `source_spread >= 3` / `>= 4` (Ticket 02's current Synchronized-story threshold): 16.8% / 10.7% — **do not use as-is, see caveat 1**.

By-quarter volume (mean / max), full table in `summary_stats.json`:

| Quarter | Clusters | Mean volume | Max volume |
|---|---|---|---|
| 2021-Q1..Q4 | ~2,900-3,100 each | 17.5-18.5 | 4,144-6,368 |
| 2022-Q1..Q4 | ~3,300-3,600 each | 17.6-18.3 | 7,917-9,004 |
| 2023-Q1..Q2 | 2,070 / 2,441 | 22.4 / 20.6 | 8,061 / 8,533 |
| 2023-Q3 | 5,547 | 23.4 | 5,142 |
| **2023-Q4** | **8,846** | **32.0** | 5,692 — *inflated by the December bulk-load artifact, caveat 3* |
| 2024-Q1..Q4 | 18-93 each | 8.3-32.0 | 23-246 — *thin, low-volume months, statistically weak* |
| 2025-Q1..Q3 | 93-1,240 | 26.8-36.4 | 1,804-8,305 — *corpus tapering off toward its 2025-09-06 stop date* |

## Findings — Russian (2025-09 → 2026-05, n=267 clusters, provisional)

| Metric | min | mean | p50 | p75 | p90 | p95 | p99 | max |
|---|---|---|---|---|---|---|---|---|
| volume | 5 | 36.1 | 7 | 14 | 32 | 81 | 775 | 976 |
| persistence (days) | 1 | 8.7 | 5 | 9 | 22 | 30 | 31 | 31 |
| source_spread (capped at 3, see caveat 4) | 1 | 1.89 | 2 | 3 | 3 | 3 | 3 | 3 |

- Share with `persistence >= 14d`: **17.2%** (3× the English share — short window, likely not yet representative).
- Share with `source_spread >= 3` (= "hit all 3 available sources"): 27.0%; `>=4` impossible here (only 3 sources exist in this archive).

## Before/after vs. currently-guessed thresholds

| Threshold (guessed, Ticket 02/03) | Guessed value | English empirical | Russian empirical (provisional) |
|---|---|---|---|
| Persistent background: `persistence >= 14d` | 14 days | Only top ~5% of clusters reach this (p95=14) — the guess sits at the tail, not the middle, of the real distribution | Top ~18% reach it — window too short to trust |
| Synchronized story: `source_spread >= 3` or `>=4` | 3 or 4 sources | **Not evaluable as computed** — 56% of the corpus (FNSPID) structurally cannot exceed source_spread=1 | Capped at 3 of 14 real sources — not comparable to a 14-source production threshold |
| Rising impulse: velocity quartile | (unspecified quartile) | **Not evaluated** — needs cross-month time series, out of this study's per-month design | Same |
| Quality-gate 30%/50% exclusion caps | 30% / 50% | **Not evaluated** — needs `quality_flags()` applied to this data | Same |

## Recommendations

1. **`persistence >= 14d` is a real outlier threshold, not a median one** — 95% of real historical clusters never reach it. If the intent was "top ~5% of stories," 14 days is well-calibrated. If the intent was closer to "the top quartile," the real p75 boundary is 5 days, not 14 — worth a short confirm-or-adjust pass with Airat rather than a full re-grill, per the ticket's own follow-up mechanic.
2. **Do not calibrate `source_spread` from the English pass at all.** The dominant sub-dataset (FNSPID, 56%) has no recoverable per-outlet signal — this is a property of the archive, not fixable by more code. Calibrate this threshold from the Russian pass instead, but treat it as provisional (3-of-14 sources only) until the live 14-source collector (`data/news_pressure/news_pressure.db`) accumulates enough real operational history to check directly — which is exactly what the ticket's own Starting Assumption already anticipated.
3. **Velocity and the 30%/50% exclusion caps remain open** — route as explicit forward notes to whoever next touches Ticket 02/03/10, rather than leaving them silently unresolved.
4. **Exclude or down-weight December 2023 in any future re-analysis of this snapshot** — it's a single dataset's load artifact, not 13× real news volume.
