# Spec: Delivery

## Purpose

Defines how and when the canonical report reaches Airat across three private surfaces, and the metric-driven decision function that determines what gets sent on any given day.

## ADDED Requirements

### Requirement: Three Private Delivery Surfaces

The system SHALL deliver reports via three surfaces, all fed by the same canonical report data, all private and single-recipient (Airat-only), with no public exposure: (1) a Telegram channel — short, condensed, phone-format content; (2) email — the full detailed rendering; (3) a local web page — the same detailed rendering as email, pull-only, no schedule of its own. Email and the local page SHALL share the same detailed rendering; Telegram SHALL use a structurally different, condensed format, not merely a shortened email. Hosting the local page on a VPS for uptime does not change its private status.

#### Scenario: Report generated

- **WHEN** a daily regimen run completes and produces a canonical report
- **THEN** the system generates a condensed Telegram message, a full-detail email, and regenerates the local page from the same underlying report, and none is exposed to any recipient other than Airat

### Requirement: Single Daily Schedule, No Intraday Alerts At v0

One regimen run per day computes `send_allowed` once and triggers both push channels (Telegram and email) together — there SHALL be no independent per-channel timers. The local page remains pull-only with no schedule. No real-time or intraday alert path exists in v0; this is explicitly deferred to a later version.

#### Scenario: Daily run completes

- **WHEN** the single daily regimen run completes
- **THEN** Telegram and email are both triggered from that one run's result, and no separate intraday push fires outside the daily schedule regardless of how sharply a signal spikes mid-day

### Requirement: 3-Level Metric-Driven Delivery Decision

Delivery per run SHALL be determined by a 3-level function, not a flat choice:

- **Level 1**: IF `send_allowed` is false (a quality-gate hard blocker), THEN send Telegram-only with a block ping, deduplicated to a short repeat-notice form when `block_reason` matches the previous day.
- **Level 2**: ELSE IF `signal_cluster_count == 0` (no cluster reached any non-noise signal type), THEN evaluate Level 3 instead of the normal product report; the local page still always regenerates with the full Technical/Ops report regardless of this branch.
- **Level 3**: a black-box function `should_notify_needs_review(run.needs_review_count)` compares `needs_review_count` against registry threshold `K` (default `1`). IF below `K`, send Telegram-only with a plain noise notice. IF at or above `K`, send Telegram-only with a dedicated needs-review ping instead of the plain noise notice.
- **Otherwise** (`send_allowed` true and `signal_cluster_count > 0`): send Telegram short digest, full email, and full local page.

`send_allowed` and `signal_cluster_count == 0` are structural existence checks and MUST NOT be placed in the parameter registry. `K` itself is a genuine tunable threshold and MUST live in the registry.

#### Scenario: Blocked run

- **WHEN** a run has `send_allowed=false`
- **THEN** Telegram-only receives a block ping, compressed if `block_reason` matches yesterday's

#### Scenario: All-noise day below the review threshold

- **WHEN** a run has `send_allowed=true`, `signal_cluster_count==0`, and `needs_review_count` below `K`
- **THEN** Telegram-only receives a plain noise notice, and the local page still regenerates the full Technical/Ops report

#### Scenario: All-noise day with unreviewed flags

- **WHEN** a run has `send_allowed=true`, `signal_cluster_count==0`, and `needs_review_count` at or above `K`
- **THEN** Telegram-only receives the dedicated needs-review ping instead of the plain noise notice

#### Scenario: Normal product day

- **WHEN** a run has `send_allowed=true` and `signal_cluster_count > 0`
- **THEN** Telegram short digest, full email, and full local page are all delivered

### Requirement: Needs-Review Never Goes Silent On An All-Noise Day

On an all-noise day, severity-(b) needs-review clusters would otherwise have no surface, since their normal home (the product report's needs-review section) is not sent that day. The system MUST fire the dedicated needs-review ping whenever `needs_review_count` clears `K`, even on an otherwise-quiet day, so the quality gate's "no silent block" rule is never violated by the delivery layer.

#### Scenario: Needs-review clusters on a quiet day

- **WHEN** `signal_cluster_count == 0` and `needs_review_count` meets or exceeds `K`
- **THEN** the system sends the needs-review ping rather than letting the flagged clusters go unmentioned

### Requirement: Delivery Threshold K Is Provisional

The default `K=1` is a starting registry value, not a validated number. It SHALL be recalibrated from real operational history via a future recalibration mechanism (the Decision Block, defined in the `evaluation` and `agentic-research-loop` capabilities) once enough operational days have accumulated. This is distinct from Ticket 12's one-time historical bootstrap: that bootstrap runs before the system has any operational history of its own, so it cannot supply the operational data `K`'s recalibration needs.

#### Scenario: Recalibration eligible

- **WHEN** enough real operational `regimen_runs` history has accumulated since `K`'s last calibration
- **THEN** a statistical read of that history is routed through the Decision Block to propose a new `K` value, rather than leaving `K` fixed indefinitely

### Requirement: Delivery Scope Reconfirmations

Two scope questions raised by earlier tickets MUST stay reconfirmed as follows, not silently reopened: multi-locale/multi-audience rendering SHALL remain out of scope — delivery stays Airat-only and Russian-only, matching the `report` capability's dictionary; and Ticket 05's legal-designation revisit condition (which triggers only if the project begins publishing/redistributing) SHALL be treated as **not** triggered by any of the three delivery surfaces, including VPS hosting of the local page, since that hosting choice is motivated by uptime only, not publicity.

#### Scenario: VPS hosting considered

- **WHEN** the local web page is hosted on a VPS for uptime reasons
- **THEN** this SHALL NOT be treated as triggering Ticket 05's publish-triggered legal-designation revisit, since hosting for uptime is not publishing or redistribution

### Requirement: Text-Only Content At v0

Every delivery channel SHALL render text-only content at v0 — no charts, no color or visual styling, including on Telegram. Chart/visual design work is explicitly deferred as a distinct, later, unresolved item.

#### Scenario: Rendering any surface

- **WHEN** the report is rendered for Telegram, email, or the local page
- **THEN** the output contains only text, with no embedded charts, images, or color/visual styling on any surface

### Requirement: Recipient Storage Behind A Stable Interface

Recipient information (Telegram channel ID, email address) SHALL be stored in a single small config file, accessed only through one stable function (e.g. `get_recipient(channel)`). No literal recipient ID SHALL appear hardcoded elsewhere in the codebase. This keeps the storage mechanism swappable (e.g. to a DB table, if a second recipient is ever needed) without touching calling code.

#### Scenario: Sending a delivery

- **WHEN** delivery code needs a channel's recipient value
- **THEN** it calls the stable accessor function rather than referencing a hardcoded ID, and the config file is the only place the actual value is stored
