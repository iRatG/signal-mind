# Spec: Quality Gate

## Purpose

Defines when a run's report is unconditionally blocked, when individual clusters are annotated or excluded instead, and the rule that no block of any kind may ever pass silently.

## ADDED Requirements

### Requirement: Unconditional Hard Blockers

The system SHALL block the entire report, in any mode (daily, weekly, monthly, history), when any of the following occur: the run failed, the collector recorded any errors, zero articles were collected, or zero clusters were built. These apply only when the input itself is missing or broken — never as a response to a quality concern about data that does exist. No partial or warned send is permitted when a hard blocker fires.

#### Scenario: Collector error present

- **WHEN** a run finishes with a collector error count greater than zero
- **THEN** the entire report SHALL be blocked from sending, with no partial send permitted

### Requirement: No Block May Ever Be Silent

Every block — hard or soft — MUST escalate to Airat with the specific reason. A blocked report MUST NOT be stored or left unsent without notice, regardless of which rule triggered the block.

#### Scenario: Any block fires

- **WHEN** a report is blocked for any reason, hard or soft
- **THEN** the system SHALL notify Airat with the specific block reason rather than leaving the report unsent silently

### Requirement: Per-Cluster Severity Replaces Whole-Report Blocking

The system SHALL evaluate quality flags per cluster, not sum them into one global report-blocking threshold. Two severity tiers MUST apply:

- **Severity (a) — annotate and keep in the main signal**: `unsupported_number` (a numeric claim in the LLM interpretation is not backed by the cluster's own evidence text); `thin_cluster` (non-daily modes only — small volume/source-spread is expected in weekly/monthly/history aggregates). The cluster stays in the report's main signal with an inline note.
- **Severity (b) — exclude from the main signal into a "needs review" section, report still sent**: `single_source_persistent` (one source, persistence at or above the registry threshold, currently 14 days); `single_source_high_volume` (one source, volume at or above the registry threshold, currently 75); `llm_fallback_heuristic` (the cluster's interpretation came from a heuristic fallback rather than a successful LLM call).

No single per-cluster flag, on its own, escalates to a full-report block — only the hard blockers do that. The 14-day and 75-volume starting values, like the 30%/50% exclusion cap below, are **provisional/open, not final calibrated numbers** — they live in the versioned registry (`analysis` capability), not hardcoded here, precisely so they can be recalibrated without a code change.

#### Scenario: Severity-a flag

- **WHEN** a cluster's LLM interpretation contains a numeric claim unsupported by its own evidence text
- **THEN** the cluster SHALL remain in the main signal with an inline `unsupported_number` annotation

#### Scenario: Severity-b flag

- **WHEN** a cluster has a single source and persistence at or above the registry threshold
- **THEN** the cluster SHALL be excluded from the main signal and moved to the "needs review" section, and the rest of the report SHALL still be sent

#### Scenario: Silent LLM fallback closed

- **WHEN** a cluster's interpretation came from the heuristic fallback instead of a successful LLM call
- **THEN** the system SHALL raise `llm_fallback_heuristic` and move the cluster into "needs review" rather than presenting the fallback interpretation as if it were a normal LLM result

### Requirement: Report-Level Exclusion-Share Cap

**Provisional/open.** A report-level cap on the share of severity-(b) exclusions — proposed starting values of 30% for daily reports and 50% for weekly/monthly/history reports — is intended to escalate a report as blocked once too many of its clusters have been excluded, rather than sending a hollowed-out report. These specific percentages were explicitly not chosen from calibrated data: Ticket 12's calibration study did not evaluate them, since doing so requires running the actual `quality_flags()` logic against historical data, which was out of that study's scope (it produced cluster-size/persistence/source_spread distributions, not a quality-flag simulation). This cap and its values MUST NOT be treated as validated or final until that simulation is run, or until a reasoned starting-value decision is explicitly made and recorded as such.

#### Scenario: Cap not yet actionable

- **WHEN** a report's severity-(b) exclusion share is computed
- **THEN** the system MUST NOT treat the 30%/50% values as a validated escalation trigger until either the `quality_flags()` historical simulation has been run, or an explicit reasoned decision to adopt a starting value has been recorded
