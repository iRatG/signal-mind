# Idea Buffer: MADPAC

Raw, unsorted ideas that surface mid-session but don't belong to one specific ticket. Not a decision log — decisions still live on their ticket's `## Working Decision`. Triage each entry into a ticket, a cross-cutting principle, or the map's `## Not Yet Specified` once implementation actually approaches it; don't let it sit here forever unsorted.

## 2026-07-31 — No hardcoded entities, registry-driven system

Captured while grilling [Ticket 10 - Market Coupling Model](tickets/10-market-coupling-model.md), but the principle is project-wide, not specific to market instruments.

Airat's framing: the system should be "беспощадна, холодна, рациональна" (unsparing, cold, rational) — no entity identifiers (instrument tickers like `IMOEX`/`RTSI`/`USD`, and by extension likely source names, signal-type labels, etc.) should ever appear as literals inside code logic. Every such entity is a row in a reference/config table, referenced only by id. Code iterates over registry rows filtered by an `active`-style flag, never branches on a hardcoded name.

This generalizes [Ticket 02](tickets/02-signal-ontology.md)'s existing cross-cutting principle ("a versioned, hierarchical parameter registry for every tunable threshold, not hardcoded") from *thresholds* to *entities themselves*. Concrete instance decided in Ticket 10: the market-instrument universe (MOEX indices, FX pairs) is a reference table sourced from the MOEX ISS API, with a pilot-subset flag (e.g. `active_in_pilot_v0`), not a list embedded in code — scaling to more instruments later is a data edit, not a code change.

Open for later: does this same registry pattern extend to news sources (Ticket 05's 14-source list), signal types (Ticket 02's 6 types), or quality-gate flag names (Ticket 03)? Not decided — worth revisiting once any of those tickets reach implementation, to check whether their current documentation-as-list approach should also become a registry table.

## 2026-07-31 — Stable interfaces between modules, free-changing internals; role independence

Captured while grilling [Ticket 10 - Market Coupling Model](tickets/10-market-coupling-model.md), specifically while discussing whether the coupling-cycle's Auditor should be a separate agent from the Runner. Airat's principle is project-wide, not specific to that one role question — recorded as a forward-note on [Ticket 11 - Agentic Delivery Process](tickets/11-agentic-delivery-process.md) since that ticket owns "which parts of the work are separate agents vs one agent with review phases," but the underlying architecture rule applies to code modules generally, not only agent roles.

Airat's framing (paraphrased): a coder and a tester must never be the same role/context, or the check "covers for itself" — role independence is non-negotiable wherever one thing is supposed to verify another. Generalized: **the project is built from modules; you change or tune the logic *inside* a module freely, but you do not rewrite how modules *interact*.** The interaction contract (interface) is the stable thing; internals are the changeable thing. This is the same "deep module" idea the `codebase-design` skill already exists to help apply (interface stability, hidden/swappable internals) — worth using that skill deliberately once any module boundary (agent roles, or a code module) is actually being drawn, rather than improvising ad hoc separation each time.

Open for later: which specific module boundaries in this project (beyond Runner/Auditor/Synthesizer) should get this same "fix the interface, free the internals" treatment? Candidates that will come up naturally: the news-pressure clustering step vs. the quality-gate step (Ticket 03), the report-rendering step vs. the signal-detection step (Ticket 04). Not decided — surface again when those modules are actually being built.

## 2026-07-31 — Fix per the documented concept, never an ad-hoc patch

Captured while grilling [Ticket 04 - Report Prototype](tickets/04-report-prototype.md), while discussing the diagnostic/support-agent role already forward-noted on [Ticket 11](tickets/11-agentic-delivery-process.md). Airat's framing: whatever breaks — a data-collection module, a scoring step, anything — gets repaired according to the project's actual specification/concept for that piece, never with a quick patch just to silence the failure. This applies at every layer he described in the same session (ops/technical logs, metrics/decision layer, product layer), not just to code the diagnostic agent touches.

This is the same discipline already implicit in the project's other standing principles — registry-driven entities instead of hardcoded literals, "no empty modules" (Ticket 02), stable module interfaces (see the entry above) — generalized explicitly to bug-fixing/incident-response behavior for the first time. Open for later: what "the documented concept" means operationally when a fix is needed fast (e.g. does the spec get consulted/updated before the fix, or does an emergency fix get retroactively reconciled with the spec afterward?) — not decided, resolve if/when Ticket 11's diagnostic-agent role is actually built.
