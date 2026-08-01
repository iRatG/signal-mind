# Ticket 08 - OpenSpec Bridge

Status: closed
Type: task
Labels: `wayfinder:task`
Claim: closed by Claude (OpenSpec compilation session, 2026-08-01)
Blocked By: Ticket 01 - Product Contract, Ticket 02 - Signal Ontology, Ticket 03 - Quality Gate Contract, Ticket 04 - Report Prototype, Ticket 05 - Source Strategy, Ticket 06 - Evaluation Harness, Ticket 07 - Delivery Policy, Ticket 10 - Market Coupling Model, Ticket 11 - Agentic Delivery Process, Ticket 12 - Historical Cluster Calibration Study — all closed, all satisfied.
Blocks: Ticket 09 - Repository Boundary (its physical `git filter-repo` extraction step, not the decision itself — already resolved)

## Question

How should the closed wayfinder decisions become an OpenSpec change proposal?

## Why This Matters

Wayfinder is for finding the route. OpenSpec is for preserving the chosen route as a durable implementation contract. This ticket prevents the methodology from ending as a chat summary.

## Task Scope

Create an OpenSpec proposal with:

- `proposal.md`: why this MVP change exists;
- `design.md`: architecture and decisions;
- `tasks.md`: implementation checklist;
- spec deltas for collection, analysis, report, quality gate, and delivery.

## Decision Shape

The ticket resolves when the OpenSpec change exists and each wayfinder decision is linked to its spec section.

## Working Decision

Resolved 2026-08-01. This was a compilation ticket, not a grilling ticket — no new decisions were made here, only already-settled decisions from Tickets 01-12 restated as a testable OpenSpec contract. Process and outcome:

**1. Scope decision, asked rather than assumed.** The pre-existing draft (`openspec/changes/news-market-coupling-agentic-loop/`, created 2026-07-28, predating the disciplined grill process) was narrowly scoped to Ticket 10/11 territory, but this ticket's own Task Scope asks for spec deltas across collection, analysis, report, quality gate, and delivery. Rather than silently deciding whether to expand that draft or fork into per-capability changes, Airat was asked directly; his framing ("что будет идеальный продукт... если мы найдём ошибку, мы по документации поймём где она... расплывчато — это много импровизации") set the actual criterion: minimize open/improvised zones, maximize traceability. **Decision: one comprehensive change**, renamed `news-market-coupling-agentic-loop` → `madpac-v0`, covering all 12 tickets' decisions as 7 capability spec deltas (`collection`, `analysis`, `quality-gate`, `report`, `delivery`, `evaluation`, `agentic-research-loop`) under one `proposal.md`/`design.md`/`tasks.md` — not 7 independent changes — because the capabilities are tightly cross-referential for a from-scratch v0 (report depends on quality-gate fields, delivery depends on report and quality-gate fields, evaluation depends on the coupling model), and splitting them would have scattered exactly the cross-cutting principles (parameter registry, severity model, Decision Block) this compilation exists to keep coherent.

**2. Compilation method.** Each of the 12 ticket files' `## Working Decision` and `## Grilling Transcript` sections was read directly (not `map.md`'s compressed gists) via parallel digest agents, one per capability, each instructed to preserve `final` vs. `provisional/open` status exactly as the source ticket or `calibration/findings-report.md` stated it — never silently upgrading a provisional number to final. The synthesis into actual `### Requirement:`/`#### Scenario:` blocks, and cross-capability coherence-checking, was kept with the main session rather than delegated, since that's the one step compression risk couldn't be outsourced.

**3. Independent verification, two rounds — the most consequential part of this ticket's process.** Rather than treat the compiled draft as final on the strength of `openspec validate --strict` alone (which only checks schema, not semantic fidelity), two separate cold-start audit sessions were run against the compiled specs and the original tickets, with no shared context with the compiling session. Round 1 found 23 real issues (1 content-level contradiction in `design.md` versus Ticket 09's own decision, 10 fabrications — invented numbers, invented field lists, misattributed decisions — and 12 omissions — real ticket decisions, including numeric thresholds, that had silently dropped out during compression). All 23 were fixed and re-verified against source text. Round 2, run after the fixes, confirmed all 23 fixes were correct (no new errors introduced during correction) and found 3 further low-severity gaps in `proposal.md`/`tasks.md` (files the first audit hadn't covered), which were also fixed. This is recorded as a validated project pattern (see `idea-buffer.md` and project memory) — the same "checker never checks its own work" principle this ticket's own `agentic-research-loop` capability mandates for the production system, demonstrated empirically on the meta-development process itself.

**4. Outcome.** `openspec/changes/madpac-v0/` is complete (`proposal.md`, `design.md`, `tasks.md`, 7 `specs/*/spec.md` files), passes `openspec validate --strict`, and every requirement is traceable to a specific ticket/section. Every value flagged provisional by a ticket or by Ticket 12's calibration (velocity quartile, `source_spread` from the English archive, `persistence>=14d`'s framing, the 30%/50% quality-gate exclusion cap, delivery's `K=1`, the agentic loop's `N=30` recalibration cadence) is stated as provisional in the spec itself, with its calibration path forward named — not silently presented as decided.

**5. Not resolved here, explicitly carried forward as open (see `design.md` Open Questions and `tasks.md`):** the relationship between `regimen_runs` and the SQLite experiment registry; whether the four-state handoff protocol applies to Runner→Auditor→Synthesizer or only Generator→Evaluator→Audit; exact MOEX instrument symbols; what happens to the original `signal-mind` paths after Ticket 09's extraction. These are implementation-time or Ticket-09-time questions, not blockers on closing this ticket.
