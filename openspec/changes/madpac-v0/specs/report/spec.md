# Spec: Report

## Purpose

Defines the canonical Markdown report shape MADPAC produces from analysis and quality-gate output, and the rules for how signal sections, review flags, and run-health information are rendered into it.

## ADDED Requirements

### Requirement: Canonical Markdown Report Shape

The system SHALL produce one canonical report as Markdown-only content, consumed as the single source of truth by every downstream delivery surface. The canonical shape includes a title/period header, an Executive Summary, one section per non-empty non-noise signal type (ordered per the canonical-to-human-label dictionary's row order, not a separately hardcoded sequence), a "needs review" section, a one-line noise/routine audit count, evidence links per representative headline, and an attributed watch-next list. No charts, color, or visual styling are part of this shape. Exact visual mode-distinction styling between report types is `delivery`/Editor-role scope, not this shape.

#### Scenario: Daily report with mixed signals

- **WHEN** a daily run produces at least one non-noise-type cluster
- **THEN** the rendered Markdown includes the title/period header, Executive Summary, one section per non-empty non-noise signal type in the dictionary's row order, the needs-review section, the noise/routine count line, and the watch-next list, with no chart or color markup anywhere in the file

### Requirement: Canonical-To-Human-Label Dictionary

Section header text MUST be read from a two-column dictionary (canonical signal type → current human-facing label, currently Russian-language), never hardcoded per report mode. The dictionary is the single source of truth for header wording. Multi-locale/multi-audience variants of the human-facing column are explicitly out of scope for v0.

#### Scenario: Rendering a section header

- **WHEN** the renderer emits the section for canonical type `Rising impulse`
- **THEN** it looks up and emits the current human label from the dictionary rather than a literal string embedded in mode-specific code

### Requirement: Noise/Routine Gets No Narrative Section

`One-source anomaly` clusters SHALL receive a real narrative section (exact field composition is an implementation detail, not fixed by this requirement). `Noise/routine` clusters SHALL NOT receive a narrative section — the report MUST render only a single audit-count line stating how many clusters were excluded as noise/unconfirmed.

#### Scenario: Noise-heavy run

- **WHEN** several clusters in a run classify as Noise/routine
- **THEN** the report's noise section contains only a one-line count, with no per-cluster narrative, headlines, or links

### Requirement: Product Report Vs. Technical/Ops Report Mode Split

WHEN a run clears the quality gate's hard blockers (articles and clusters exist, no collector failure) but no cluster reaches any of the 5 non-noise signal types, the system SHALL NOT send a product report. It SHALL instead send a visually distinct Technical/Ops report stating that the run completed normally but found nothing reportable, carrying the run's overall-progress/meta-functionality content. This is distinct from a hard-blocker block, which covers data absence rather than data-present-with-no-signal. Both the product report and the Technical/Ops report remain Airat-only at this stage, with no external delivery implication.

#### Scenario: All-noise day

- **WHEN** a run collects articles and builds clusters, and none of the clusters reach any of the 5 non-noise signal types
- **THEN** the system emits a Technical/Ops report stating the run completed normally, and does not emit a product report for that period

### Requirement: Needs-Review Visibility In The Product Report

The quality gate's severity-(b) "needs review" clusters (`single_source_persistent`, `single_source_high_volume`, `llm_fallback_heuristic`) MUST remain visible in the product report as their own dedicated section, since they represent a specific finding worth attention, not a run-health metric.

#### Scenario: Flagged cluster present

- **WHEN** a cluster is flagged `single_source_persistent` or `llm_fallback_heuristic`
- **THEN** the product report's needs-review section lists that cluster with its flag and a short explanation, per the exclusion behavior already defined by the `quality-gate` capability's severity-(b) model

### Requirement: Run-Health Metrics Stay Out Of The Product Report

Run-level health metrics (`send_allowed`, `article_count`, `quality_flag_count`, `source_counts`, and similar) SHALL NOT get a dedicated section in the product report. They belong at the ops/technical tier — the Technical/Ops report — separate from the analysis/decision tier and the product tier.

#### Scenario: Healthy run

- **WHEN** a run computes `quality_flag_count: 0` and `send_allowed: true`
- **THEN** these values appear only in the Technical/Ops report's technical summary, never as a section in the product report

### Requirement: Evidence Links

Each signal section SHALL list its representative headlines as Markdown links using the already-populated `article.url` field, in the form `[headline](url) — source, date`. This is rendering only; it requires no new data collection.

#### Scenario: Rendering a representative headline

- **WHEN** a signal section's representative article has a populated `url` field
- **THEN** the report renders it as a Markdown link with source and date, not as plain unlinked text

### Requirement: Attributed Watch-Next List

Per-cluster watch-next sentences SHALL be aggregated into a single list, each item tagged with the human-facing label (from the canonical-to-human-label dictionary) of the signal it originated from. They SHALL NOT be blended into one unattributed summary paragraph.

#### Scenario: Multiple signals with watch-next items

- **WHEN** two different clusters each have their own watch-next sentence
- **THEN** the watch-next section lists them as separate bullet items, each prefixed with its originating signal's human-facing label (e.g. "— [Главный импульс] ...")

### Requirement: Fact-Based Executive Summary

The Executive Summary MUST open with exactly one lead sentence naming the period's single most notable finding using a concrete fact or number, never an evaluative adjective in place of a fact. It MUST be followed by a bullet list giving one line per non-empty section with its item count.

#### Scenario: Notable Main-pressure finding

- **WHEN** the period's Main pressure signal has a specific source count and time window that stands out versus recent history
- **THEN** the Executive Summary's lead sentence states that fact concretely, followed by a bullet-count list of the other non-empty sections
