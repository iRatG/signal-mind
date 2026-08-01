# Proposal: MADPAC v0

Status: draft
Created: 2026-07-28
Revised: 2026-08-01 (expanded from the original `news-market-coupling-agentic-loop` scope to cover the full MADPAC v0 MVP)

## Why

MADPAC has spent 2026-07-28 through 2026-08-01 as a wayfinder-and-grilling decision process: 12 tickets, each resolved live with Airat, covering source strategy, signal classification, quality gating, report shape, delivery, evaluation, the agentic research loop, and the repository boundary. Those decisions exist today as a narrative archive scattered across 12 ticket files plus `idea-buffer.md` — durable as a record of *why*, but not directly executable as a build contract, and with no single place that states *what the system must do right now*, testably.

This proposal is the compression step: it takes every closed wayfinder decision and restates it as a spec a coding agent can implement against and a reviewer can check work against — without re-reading grilling transcripts to reconstruct current truth. It does not re-decide anything. Where a ticket left a number or mechanism explicitly provisional (not yet grilled, not yet calibrated), this proposal preserves that status rather than silently finalizing it.

## What Changes

- Define the full MADPAC v0 daily analytical loop as one integrated system, not independent shippable features: collection → analysis (signal classification + market coupling) → quality gate → report → delivery, plus the evaluation harness that watches the whole loop and the agentic process that builds and operates it.
- Define 7 capability spec deltas: `collection`, `analysis`, `quality-gate`, `report`, `delivery`, `evaluation`, `agentic-research-loop`.
- Define the versioned parameter/entity registry and Metric Card mechanisms that every numeric threshold and metric across the system must live in — never hardcoded.
- Define the per-cluster severity model that replaces all-or-nothing report blocking, and the escalation rule that no block, hard or soft, may ever be silent.
- Define the Decision Block as the single mechanism behind every calibration decision in the system (classification-mismatch calibration, quality-gate diagnosis, champion/challenger promotion, delivery-threshold recalibration).
- Preserve every value a ticket or Ticket 12's calibration study flagged as provisional/not-yet-evaluable exactly as provisional — most importantly: `source_spread` thresholds are not usable from the English calibration archive at all (a structural archive limitation, not a bug); `persistence>=14d` sits near the real p95 boundary and needs a confirm-or-adjust pass, not a re-grill; velocity quartiles and the 30%/50% quality-gate exclusion caps were never evaluated; delivery threshold `K` and the N=30 recalibration cadence are explicit starting values pending real operational history.
- Rename the OpenSpec change directory from `news-market-coupling-agentic-loop` to `madpac-v0`, reflecting that its scope is now the whole MVP, not only the market-coupling research loop.

## Capabilities

### New Capabilities

- `collection`: which news sources are active, how each is reached (RSS/archive/VPN routing), the label-not-gate legal-designation policy, source categorization, and the pluggable ingestion contract.
- `analysis`: the 6 canonical signal types, the versioned parameter/entity registry, the market-coupling model (registry-driven instruments, event windows, anomaly-metric-vector approach), and the calibration status of every threshold involved.
- `quality-gate`: unconditional hard blockers, the never-silent escalation rule, and the per-cluster severity model (annotate-and-keep vs. exclude-into-review) that replaces whole-report blocking.
- `report`: the canonical Markdown report shape, the canonical↔human-label dictionary, the Product-report vs. Technical/Ops-report mode split, evidence links, and the fact-based Executive Summary rule. Both report types stay Airat-only at v0; privacy/delivery scope itself is owned by the `delivery` capability, not restated here.
- `delivery`: the three private delivery surfaces (Telegram, email, local page), the single daily schedule, the 3-level metric-driven delivery-decision function, and the recipient-storage interface pattern.
- `evaluation`: the two-track harness (statistical эталон backtest + qualitative rubric review), the Metric Card registry, and the Decision Block's role as the system's one unified calibration mechanism.
- `agentic-research-loop`: the three-role production research cycle (Runner/Auditor/Synthesizer), the meta-development process (wayfinder → grilling → openspec → implement, with satellite skills activating only after `tasks.md` exists), and the champion/challenger + threshold-recalibration feedback contour. The four-state handoff protocol's application to the Runner/Auditor/Synthesizer cycle specifically (as opposed to Generator/Evaluator) remains an open question — see `design.md`'s Open Questions — not yet a fully closed part of this capability.

### Modified Capabilities

None — `openspec/specs/` is currently empty; this is the first compilation of MADPAC decisions into OpenSpec, so every capability above is new relative to the archived main specs.

## Impact

- Affected docs: all 12 files under `docs/wayfinder/news-pressure-radar/tickets/`, `idea-buffer.md`, and `calibration/findings-report.md` are the source material this proposal compiles from; they remain the permanent decision archive and are not replaced or deleted.
- Affected code (future implementation, not this compilation step): `analytics/news_pressure_cluster.py`, `scripts/news_pressure_regimen.py`, `config/news_pressure_sources.yaml`, `analytics/news_pressure_snapshot.py`, `data/news_pressure/`, `src/agent/agent.py` and `src/agent/experiments.py` (reused Ouroboros SQL-hypothesis pattern).
- No production code changes ship as part of this proposal itself — it is a planning artifact. Implementation follows via `tasks.md` once this proposal, its specs, and its design are reviewed.
- This change is expected to be the one migrated whole, with full git history, into the independent `github.com/iRatG/madpac` repository per Ticket 09's decision, once Ticket 12's snapshot work (closed) and this compilation (this proposal) are both done.
