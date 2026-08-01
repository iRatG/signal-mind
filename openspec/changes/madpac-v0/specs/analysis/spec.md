# Spec: Analysis

## Purpose

Defines how collected news is classified into 6 canonical signal types and how a market anomaly is traced back to news pressure, including which thresholds are final and which remain provisional pending calibration.

## ADDED Requirements

### Requirement: Versioned Parameter And Entity Registry

Every numeric threshold and every reference entity used anywhere in analysis (signal-type thresholds, instrument lists, event-window sizes) MUST live in a versioned registry — grouped hierarchically by related metric/entity family, each entry carrying its current value, description, and change history — never hardcoded as a literal in application logic. Recalibrating a registry value from accumulated operational history is a separate mechanism (the Decision Block, `evaluation` capability); this requirement only guarantees the value and its history are stored so recalibration is possible.

#### Scenario: Changing a threshold

- **WHEN** a developer needs to change a signal-type threshold or add a new market instrument
- **THEN** they edit a registry entry (value + change history), and no application code changes

### Requirement: No Empty Modules

Where a classification rule cannot be fully resolved today, the system MUST still define a concrete, simple, best-practice-grounded starting rule — never a stub or placeholder — explicitly flagged in the registry as provisional and revisitable.

#### Scenario: Unresolved sub-rule

- **WHEN** a new noise-filtering sub-category is needed but has not been empirically validated
- **THEN** the system defines a concrete starting rule (e.g. a named token list) rather than leaving the category unimplemented

### Requirement: State Transitions Over Permanent Labels

A cluster's signal-type classification MUST be able to change as evidence accrues: a Rising-impulse cluster holding for 3 consecutive months reclassifies to Persistent background; an unconfirmed One-source anomaly reclassifies to Noise/routine if its follow-up window closes without corroboration or market confirmation; a nominally-routine cluster is rescued out of Noise/routine if it coincides with an abnormal market move.

#### Scenario: Anomaly ages out unconfirmed

- **WHEN** a One-source anomaly's follow-up window closes with no additional source corroboration and no abnormal market move
- **THEN** the system reclassifies the cluster as Noise/routine rather than leaving it permanently labeled as an anomaly

### Requirement: News-Plane × Market-Plane Cross-Check

A news-side pattern alone (high pressure score, high source spread) SHALL NOT be treated as evidence of market relevance. The system MUST treat it as market-relevant only once it coincides with an abnormal market move in the event window. This check MUST be reused, not reimplemented, across Main pressure, Synchronized story, One-source anomaly, and the Noise/routine rescue path, and MUST pass before any signal type triggers out-of-cadence delivery.

#### Scenario: News-only pattern with no market move

- **WHEN** a cluster shows a high news-only pressure score with no corresponding abnormal market move in its event window
- **THEN** the report frames it as news-side pressure only, and it does not trigger out-of-cadence delivery

### Requirement: Signal Type — Main Pressure

A cluster classifies as Main pressure when it shows high volume and broad source spread. Report language MUST use retrospective/historical-strength framing only, never a directional forecast. It MAY trigger out-of-cadence delivery, but only once market-confirmed via the news-plane × market-plane cross-check.

#### Scenario: Market-confirmed main pressure

- **WHEN** a cluster shows high volume and broad source spread, and the market cross-check finds an abnormal move in the event window
- **THEN** the system may trigger out-of-cadence delivery, using historical-strength language only

### Requirement: Signal Type — Rising Impulse

The system SHALL classify a cluster as Rising impulse when its velocity score sits in the top quartile of the trailing velocity distribution (a registry value), subject to a minimum absolute volume guard against small-denominator artifacts, judged against 1/2/3-month trailing aggregates. Holding for 3 months MUST reclassify the cluster to Persistent background. Rising impulse MUST NOT trigger independent delivery.

The velocity-quartile threshold is **provisional/open**: Ticket 12's calibration ran clustering independently per calendar month, producing no cross-month time series, so no empirical velocity threshold exists yet. This requirement's registry value is a starting point, not a validated number.

#### Scenario: Velocity holds across months

- **WHEN** a topic's velocity score sits in the top quartile for 2 consecutive months
- **THEN** the system flags it as Rising impulse (credible); **WHEN** it holds for a 3rd month, **THEN** it reclassifies to Persistent background

### Requirement: Signal Type — Persistent Background

A cluster classifies as Persistent background when its persistence reaches the registry threshold, currently 14 days. A recurring routine rubric with unchanging phrasing MUST be filtered out via a collocation/word-combination stability check, not persistence alone — if the surrounding language shifts, it is treated as a genuinely evolving story. This section appears only at the weekly report layer, never daily, and never triggers delivery.

The `persistence>=14d` value is **provisional/open in framing, not in mechanism**: Ticket 12's calibration found this threshold sits at approximately the p95 boundary of real cluster lifespans (median 3 days, p75 5 days) — it marks roughly the rarest 5% of stories, not a mid-range cutoff. If a "top ~5%" framing was intended, the value needs no change; if a broader "top quartile" framing was intended, the empirical p75 boundary is closer to 5 days. This requires a short confirm-or-adjust pass with Airat, not a full re-grill, and has not yet happened.

#### Scenario: Persistent theme with stable language

- **WHEN** a theme's cluster persists 14+ consecutive days with unchanging surrounding language
- **THEN** the system classifies it as Persistent background and surfaces it only in the weekly report, not daily

### Requirement: Signal Type — Synchronized Story

A cluster classifies as Synchronized story when its source spread reaches the registry threshold (currently `>=3` for daily/weekly, `>=4` for monthly/history), distinguishing independent corroboration (sources describing the theme in their own words) from copy-paste republishing of the same press-release text via a lexical-diversity check. Delivery requires market confirmation, same as Main pressure.

The `source_spread` thresholds are **provisional/open and explicitly not validated from the English calibration archive**: 56% of that corpus (`fnspid_news`) has no recoverable per-outlet identity — every article resolves to the same re-syndication domain, so any cluster it dominates shows `source_spread=1` by construction. This is a structural property of that archive, not a fixable bug, and that corpus MUST NOT be used to calibrate this threshold. The Russian pass (27.0% of clusters reached `>=3`) is itself only provisional — capped at 3 of the eventual 14 production sources, not comparable to a 14-source threshold. This threshold SHALL be recalibrated from live 14-source Russian production history once enough has accumulated, never from the English archive.

#### Scenario: Independently corroborated story

- **WHEN** a story is reported by 3 or more sources in production data with distinct phrasing (not copy-paste republication)
- **THEN** it qualifies as Synchronized story

#### Scenario: Attempted calibration from the wrong corpus

- **WHEN** someone attempts to calibrate or validate `source_spread` thresholds using the English `hf_news.db` archive
- **THEN** the result MUST be treated as invalid, since 56% of that corpus cannot report a real source count

### Requirement: Signal Type — One-Source Anomaly

The system SHALL classify a cluster as "watch" when its source spread equals 1. It MUST reclassify to a confirmed one-source anomaly if corroborated by 2 or more additional sources later, or if it coincides with an abnormal market move within the follow-up window; otherwise it MUST reclassify to Noise/routine. Source category (state-aligned / independent-opposition / foreign-by-country, from the `collection` capability) MUST be treated as an additional modifier on top of — never a replacement for — the follow-up/market check: coverage limited to independent-opposition media while state-aligned media stay silent (or the reverse) is itself informative, not disqualifying. Delivery MUST require market confirmation.

#### Scenario: Unconfirmed one-source story

- **WHEN** a story is reported by only one source and the follow-up window closes with no additional corroboration and no market move
- **THEN** the system reclassifies it as Noise/routine

#### Scenario: Confirmed by market move

- **WHEN** a one-source story coincides with an abnormal market move despite lacking further source corroboration
- **THEN** the system confirms it as a genuine one-source anomaly

### Requirement: Signal Type — Noise/Routine

A cluster classifies as Noise/routine when it matches routine-token categories — currently weather/horoscope, financial boilerplate (currency-rate blurbs, market-open/close notices), and calendar fillers (TV schedules, church calendar, sports schedules) — combined with the collocation-stability check, or when it fails to qualify for any other signal type (an unconfirmed One-source anomaly, a seasonal small-denominator Rising-impulse candidate, a copy-paste Synchronized story). It receives no narrative section, only an audit count. A nominally-routine cluster coinciding with an abnormal market move MUST be rescued and reclassified rather than suppressed. A mostly-noise period is itself a quality-gate flag.

#### Scenario: Routine topic rescued by market relevance

- **WHEN** a routine-classified topic coincides with an abnormal market move
- **THEN** the system rescues it out of Noise/routine and reclassifies it under the appropriate signal type instead of suppressing it

### Requirement: Registry-Driven Instrument Universe

Market instruments MUST be rows in a reference table (seeded from the MOEX ISS API: `IMOEX`, `RTSI`, 10 sector indices, 3 FX pairs), never literals inside application code. An `active_in_pilot_v0` flag marks the v0-active subset (`IMOEX` plus a small set of news-sensitive sector indices, plus all 3 FX pairs; `RTSI` starts inactive as it duplicates `IMOEX` in dollar terms). Expanding the instrument set is a data edit, not a code change.

#### Scenario: Adding a new instrument

- **WHEN** a new sector index needs to be added to the pilot
- **THEN** an operator sets `active_in_pilot_v0` on its registry row; no code changes are required

### Requirement: Coupling Direction Is Market-First For v0

The system SHALL start from a market anomaly on date D and inspect news pressure in the surrounding window. News-first and bidirectional analysis MUST remain deferred to a later stage, pending the news side having a calibrated historical baseline. Ticket 12 (closed) already ran; it left this baseline gap open as a follow-up finding, not as unfinished ticket scope.

#### Scenario: Market anomaly detected

- **WHEN** an instrument shows an abnormal move on date D
- **THEN** the system looks at news pressure in the D-3..D+3 window around it, not the reverse direction

### Requirement: Event Window

The event window is D-3..D+3, with D+5 available as an optional extended horizon. Window sizes MUST live in the same registry as other analysis thresholds, never hardcoded.

#### Scenario: Standard window evaluation

- **WHEN** evaluating a market anomaly on date D
- **THEN** the system pulls news-pressure data for D-3 through D+3 by default, and additionally D+5 only when the extended-horizon flag is set

### Requirement: Anomaly Detection Uses A Metric-Vector Pool

The system MUST NOT determine a market anomaly from a single metric or a fixed pass/fail gate. It SHALL evaluate a small pool of candidate metrics (e.g. top-N% absolute daily move over a quarter, deviation beyond a volatility band, relative move versus `IMOEX` for sector indices) and SHALL record their raw values as a vector per instrument/date. Metrics MAY be instrument-specific or topic-specific; this is expected, not a defect.

This requirement's exact metric list, count, and combination logic are **provisional/open**: Ticket 12's per-month clustering design produced no cross-month time series, so the anomaly-metric pool remains uncalibrated. This is a real, flagged gap for future calibration work, not a silently guessed number.

#### Scenario: Recording an anomaly evaluation

- **WHEN** the system evaluates whether a given date is anomalous for an instrument
- **THEN** it records the raw values of every pool metric as a vector, never reducing the result to a single pass/fail boolean

### Requirement: News-Side Meaningfulness Stays Descriptive At v0

For every market anomaly, the system records source count, headline count, and same-day multi-source synchronization as descriptive facts, without applying a numeric significance cutoff at v0. Anomalies with weak or no visible news coverage MUST be kept as explicit negative cases, not discarded.

#### Scenario: Anomaly with no visible news coverage

- **WHEN** a market anomaly has zero or minimal recorded news coverage in its window
- **THEN** the system stores the anomaly row with `negative_case=true` rather than dropping it from the dataset

### Requirement: Text-Depth Staging For Market-Coupling Evaluation

Headlines SHALL be the default corpus for the full anomaly scan. Descriptions/leads SHALL be used only where already present in already-collected data. Full article text SHALL be fetched only for a small number of candidates where a strong market anomaly has an ambiguous or uninformative headline.

#### Scenario: Ambiguous strong anomaly

- **WHEN** a market anomaly is strong but its peak-day headlines are vague or uninformative
- **THEN** the system fetches full article text for that small candidate set only, not for the general scan

### Requirement: No Causal Language

Relation and interpretation text MUST use terms like "coincides with" or "candidate relation," never "caused," "proved," or "predicts." This applies to every signal type and to market-coupling output alike.

#### Scenario: Generating relation text

- **WHEN** the system generates the `relation_note` field for a market-coupling output row
- **THEN** the text uses no causal verbs, consistent with retrospective-only framing

### Requirement: Market-Coupling Output Row Schema

Every market-coupling evaluation MUST produce a row with exactly: `instrument_id, event_date, window, metric_vector, news_source_count, news_headline_count, news_sync_flag, news_peak_timing, relation_note, negative_case`. This is the raw evidence record; human-facing rendering is defined separately in the `report` capability.

#### Scenario: Runner completes an evaluation

- **WHEN** the Runner finishes evaluating an instrument for an event date
- **THEN** it emits exactly this row shape, leaving presentation to a separate downstream step
