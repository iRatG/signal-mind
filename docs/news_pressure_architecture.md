# News Pressure Radar: technical plan

## Product frame

News Pressure Radar is not a news aggregator. It is a system for measuring how
the public agenda changes over time:

- which themes accelerate;
- which themes fade;
- which stories appear across several sources at once;
- which stories are pushed by one source while others stay silent;
- which words and named entities suddenly become more frequent;
- which themes persist for days or weeks.

The first useful interface is a daily Telegram or Markdown report. A dashboard
comes later, after the report proves that the signal is valuable.

## Repository strategy

Use `signal-mind` as an incubator because it already contains news loaders,
signal analysis ideas, and data-oriented conventions.

Keep the work isolated on a dedicated branch:

```bash
git switch -c news-pressure-radar-spike
```

When the core stabilizes, extract it into a separate GitHub repository, for
example:

- `news-pressure-radar`
- `agenda-pressure`
- `pulse-radar`
- `povestka-radar`

The extracted repository should not carry the broader market-agent code from
`signal-mind`.

## Current spike

Current file:

```text
analytics/news_pressure_snapshot.py
analytics/news_pressure_cluster.py
```

Current artifacts:

```text
data/news_pressure/news_pressure.db
data/news_pressure/headlines_YYYY-MM-DD_YYYY-MM-DD.jsonl
data/news_pressure/pulse_YYYY-MM-DD_YYYY-MM-DD.md
data/news_pressure/cluster_pulse_YYYY-MM-DD_YYYY-MM-DD.md
data/news_pressure/semantic_cluster_pulse_YYYY-MM-DD_YYYY-MM-DD.md
data/news_pressure/agenda_pulse_YYYY-MM-DD_YYYY-MM-DD.md
data/news_pressure/agenda_pulse_v2_YYYY-MM-DD_YYYY-MM-DD.md
data/news_pressure/errors_YYYY-MM-DD_YYYY-MM-DD.json
```

Verified month-depth run:

```text
period: 2026-06-22..2026-07-21
sources: kommersant, interfax, lenta, vedomosti, ria
fetched unique headlines after global URL dedup: 4476
normalized rows: 4476
baseline clusters: 15
local similarity graph clusters: 15
LLM-labeled agenda pulse: yes
errors: 0
```

Verified three-month history run:

```text
period: 2026-04-23..2026-07-21
sources: kommersant, interfax, lenta, vedomosti, ria
headline rows in SQLite after run: 13219
normalized rows in SQLite after run: 13219
local similarity graph clusters: 20
LLM-labeled agenda pulse v2: yes
errors: 0
```

RBC is listed in `config/news_pressure_sources.yaml`, but disabled for now:
`www.rbc.ru` returns Qrator `401/403` to this server. Do not count it as an
active source until a stable archive/API/feed path is found.

TASS was checked as another famous agency source, but currently returns `403`
to this server. `ria` is the active replacement because it has a stable archive
shape like `https://ria.ru/20260721/`.

## Data principle

Store raw headlines first. Analysis is a derived layer.

Do not make early runs depend on full article text. Full text introduces legal,
parsing, paywall, and boilerplate problems. Headlines are enough for the first
agenda-pressure model.

## SQLite schema

Current minimal schema:

```text
sources(
  name primary key,
  archive_pattern,
  enabled
)

headline_snapshots(
  id primary key,
  source,
  published_date,
  title,
  url unique,
  title_hash,
  collected_at,
  raw_meta
)

snapshot_runs(
  run_id primary key,
  period_from,
  period_to,
  sources,
  started_at,
  finished_at,
  fetched_count,
  inserted_count,
  error_count
)

snapshot_errors(
  run_id,
  source,
  published_date,
  error,
  created_at
)
```

Next schema additions:

```text
normalized_titles(
  article_id,
  normalized_title,
  language,
  entities_json
)

topic_runs(
  run_id primary key,
  period_from,
  period_to,
  method,
  params_json,
  created_at
)

topics(
  topic_id primary key,
  run_id,
  label,
  summary,
  pressure_score,
  volume,
  velocity,
  persistence,
  source_spread,
  novelty,
  silence_gap
)

article_topics(
  article_id,
  topic_id,
  confidence
)
```

## Pipeline

### 1. Collect

Input:

- archive pages;
- date range;
- source list.

Output:

- raw headline rows in SQLite;
- JSONL export for quick inspection;
- errors table for reproducibility.

Rules:

- idempotent reruns;
- URL dedup first;
- reject URLs with an embedded date that does not match the archive date;
- later add fuzzy title dedup;
- every run gets a `run_id`.

### 2. Normalize

Normalize headlines before analysis:

- lowercase;
- remove source boilerplate;
- strip dates and counters;
- tokenize;
- extract named entities if available;
- store normalized text separately.

This layer must be inspectable. If topic quality is bad, the normalized rows
should make the reason visible.

### 3. Cluster

Initial options:

- baseline keyword groups for sanity checks;
- embeddings for real clustering;
- optional LLM labels after clustering, not before.

Recommended direction:

```text
title -> embedding -> clustering -> cluster metrics -> LLM label/summary
```

Avoid pure LLM classification for every headline at the start. It is expensive,
less reproducible, and harder to debug.

### 4. Score pressure

The first transparent score was:

```text
pressure_score =
  0.35 * normalized_volume +
  0.25 * velocity +
  0.20 * source_spread +
  0.10 * persistence +
  0.10 * novelty
```

Where:

- `volume`: number of headlines in the cluster;
- `velocity`: recent share minus previous-window share;
- `source_spread`: number of sources covering the topic;
- `persistence`: number of active days in the period;
- `novelty`: how unusual the cluster is versus the trailing month.

`silence_gap` should be reported separately because it is not always positive or
negative. Sometimes one-source pressure is propaganda/noise; sometimes it is an
early signal.

Current implementation uses `pressure_score_v2`:

```text
pressure_score_v2 =
  (
    0.25 * volume_score +
    0.25 * velocity_score +
    0.20 * source_spread_score +
    0.15 * persistence_score +
    0.15 * novelty_score
  ) * (1 - noise_penalty)
```

Where:

- `volume_score`: log-scaled cluster size;
- `velocity_score`: recent daily rate versus the previous week;
- `source_spread_score`: share of active sources covering the cluster;
- `persistence_score`: active days divided by period days;
- `novelty_score`: recent share versus historical share;
- `noise_penalty`: routine or one-source noise penalty;
- `anomaly_score`: separate diagnostic score for narrow, unusual signals.

The reader-facing report prints these components for each cluster so a user can
see why a story is ranked as main pressure, rising, persistent, synchronized, or
noise.

### 5. Report

Daily report shape:

```text
Pulse of the day

1. Main pressure cluster
2. Rising clusters
3. Fading clusters
4. Cross-source synchronized stories
5. One-source anomalies
6. Words/entities that spiked
7. What to watch tomorrow
```

The report should include links to representative headlines so the analysis can
be audited quickly.

Current report layers:

- `semantic_cluster_pulse_*.md`: audit report with technical labels, metrics,
  representative headlines, and LLM/heuristic interpretation fields.
- `agenda_pulse_*.md`: reader-facing pulse report with main signal, rising
  themes, synchronized stories, persistent background, anomalies/noise, and
  what to watch next.
- `agenda_pulse_v2_*.md`: reader-facing pulse with section-level deduplication,
  an executive summary, and audit flags such as `main`, `rising`,
  `synchronized`, `persistent`, and `anomaly/noise`.

Quality gate rules in the current v2 report:

- LLM interpretations are checked for numbers that are not supported by
  representative headlines.
- Russian number words such as `пять` are normalized for claim checks.
- Rounded market thresholds such as `above $90` can be supported by evidence
  like `$91.32`.
- persistent one-source high-volume clusters are marked as `needs_review` and
  routed to `Аномалии И Шум`, not `Устойчивый Фон`.

### 6. Deliver

Delivery is downstream from analysis:

- Markdown file first;
- Telegram DM or MetaStore report second;
- dashboard only after repeated useful reports.

### 7. Regimen

The operational unit is one idempotent run:

```text
collect window -> update SQLite -> rebuild cluster report -> write Markdown
```

Use `scripts/news_pressure_regimen.py` as the cron-ready entrypoint:

```bash
python scripts/news_pressure_regimen.py --mode daily --as-of 2026-07-22
python scripts/news_pressure_regimen.py --mode weekly --as-of 2026-07-22
python scripts/news_pressure_regimen.py --mode monthly --as-of 2026-07-22
python scripts/news_pressure_regimen.py --mode history --as-of 2026-07-22
```

Modes:

- `daily`: yesterday only, for the morning pulse;
- `weekly`: trailing 7 completed days, for context;
- `monthly`: trailing 30 completed days, for product-quality review;
- `history`: trailing 90 completed days, for statistics and baseline drift.

The regimen passes the selected mode into `analytics/news_pressure_cluster.py`
as `--report-mode`. The report composer then changes both selection priorities
and section names:

- `daily`: `Пульс Дня`; emphasizes fresh impulses with lower cluster thresholds
  and no `thin_cluster` penalty for 2-headline clusters.
- `weekly`: `Пульс Недели`; emphasizes developing stories and uses a lower
  cross-source synchronization threshold than long-window reports.
- `monthly`: `Пульс Месяца`; balances rising stories, synchronized themes, and
  persistent background.
- `history`: `Пульс Повестки: 90 Дней`; treats the window as a statistical
  baseline and separates long background from fresh acceleration.

For `daily` and `weekly`, cross-source synchronization starts at 3 sources. For
`monthly` and `history`, it starts at 4 sources. Empty daily sections are
omitted to keep the morning report compact.

Cron should call the regimen script only after report quality is acceptable.
Telegram delivery should remain a separate downstream step so collection and
analysis can be debugged without sending noisy reports.

Each regimen run now also writes a machine-readable status layer:

```text
data/news_pressure/run_status/regimen_YYYYMMDDTHHMMSSffffffZ.json
data/news_pressure/run_status/latest_daily.json
data/news_pressure/run_status/latest_weekly.json
data/news_pressure/run_status/latest_monthly.json
data/news_pressure/run_status/latest_history.json
```

The same status is stored in SQLite table `regimen_runs`.

Status fields include:

- `status`: `ok` or `error`;
- `snapshot_run_id` and `topic_run_id`;
- `fetched_count`, `inserted_count`, `article_count`, `cluster_count`;
- `source_counts`;
- `quality_flag_count`;
- `report_path`;
- `send_allowed`;
- `block_reason`.

`send_allowed` is the delivery gate for future Telegram automation. It is true
only when the run completed, there were no collector errors, there are articles
and clusters, and the quality flag count is within `--max-quality-flags`.

## Three-month depth test

Use three months as the standing historical baseline. It is not just a one-off
report; it is the window that lets the system compare fresh pressure against a
meaningful recent history.

Expected rough size from current run:

```text
1 month: ~5.3k fetched headlines, ~4.0k unique rows
3 months: ~16k fetched headlines, ~12k unique rows
```

This is small enough for SQLite, embeddings, and local experiments.

## Near-term checklist

1. Keep daily/weekly/monthly/history regimen runs stable for 3-5 days without
   manual fixes.
2. Add Telegram delivery as a downstream step gated by `send_allowed`.
3. Improve source/category noise rules.
4. Add proper Russian lemmatization or embeddings when the local graph becomes
   the limiting factor.
5. Decide whether to extract into a new GitHub repository after the first stable
   multi-day regimen cycle.

Current status for this checklist:

- RBC is present in config but disabled because of Qrator blocking.
- Source definitions are in `config/news_pressure_sources.yaml`.
- `normalized_titles` exists and stores normalized tokens plus a fuzzy key.
- Baseline lexical cluster reports exist as `cluster_pulse_*.md`.
- A lightweight canonical-token layer exists and already merges simple forms
  like `иран`/`ирана`.
- `analytics/news_pressure_cluster.py` exists as a separate analysis layer. It
  reads SQLite, builds deterministic local similarity-graph clusters, writes a
  `local_similarity_graph_v1` topic run, and emits
  `semantic_cluster_pulse_*.md`.
- `analytics/news_pressure_cluster.py` supports `--label-mode deepseek` for LLM
  labels and falls back to deterministic heuristic labels if the API is
  unavailable. The same run also writes `agenda_pulse_*.md`.
- `agenda_pulse_v2_*.md` adds section-level deduplication so one strong cluster
  does not appear in too many digest sections.
- `scripts/news_pressure_regimen.py` provides a single repeatable command for
  daily, weekly, monthly, and 90-day history runs.
- A first quality gate exists: unsupported numeric claims and persistent
  one-source high-volume clusters are flagged for review.
- `pressure_score_v2` exists and is stored in `topic_runs.params_json` as
  `score_version=pressure_score_v2`. The v2 reports show score components next
  to each story.
- Next quality jump: proper lemmatization, source/category noise rules, daily
  versus weekly/monthly report modes, and real embedding-based clustering. The
  local graph is useful as a reproducible baseline, but broad geopolitical
  stories still need a semantic model.
