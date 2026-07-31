# Ticket 11 - Agentic Delivery Process

Status: open
Type: grilling
Labels: `wayfinder:grilling`
Claim: unclaimed
Blocks: Ticket 08 - OpenSpec Bridge, Ticket 09 - Repository Boundary

## Question

What multi-agent process should this project use so the work itself becomes a reusable methodology?

## Why This Matters

Airat clarified that News-Market Coupling is not only a data idea. It is also a training ground for learning how to use skills and independent agents correctly: one agent clarifies a foggy idea, another turns it into a technical/spec artifact, another implements, another checks quality, and the cycle repeats if the result is weak.

## Working Decision

The desired process is iterative:

- clarify the foggy idea;
- create a decision map;
- write a technical/spec artifact;
- implement a narrow spike;
- review quality independently;
- accept, reject, or iterate;
- preserve the decision and learning in memory/spec/docs.

## Decision Shape

Define:

- which roles exist: wayfinder, researcher, spec writer, implementer, tester, reviewer, summarizer;
- which artifacts each role produces;
- when a task is allowed to move from idea to code;
- what quality gate accepts a spike;
- what happens when a review fails;
- how to keep the process lightweight enough for a solo project.

## Starting Assumption

Use a small process first: `wayfinder/grill -> spike spec -> implementation -> independent review -> grill-up summary -> memory/spec update`. Add more agents only when parallel work is genuinely useful.

Note (2026-07-31, not yet resolved): `grill-up` above is a placeholder that doesn't map to a real skill — confirmed against the `mattpocock/skills` source tree, no such skill exists. Needs replacing when this ticket is actually resolved.

Airat expanded the loop unprompted while grilling Ticket 01: идея → обсуждение/прожарка → спецификация → метрики → проверка → код → итерация → аудит/сверка метрик → фиксация результатов → изменение → применение изменений → следующая итерация. Compared to the Working Decision above, this adds two steps not yet present: explicit **metrics** as their own artifact (not folded into "review quality"), and an explicit **audit/metrics-reconciliation** step distinct from initial review. This project's main deliverable, in Airat's words, is this skeleton/framework itself — implementation detail is delegated to `grilling`/`grill-me`, `wayfinder`, `openspec`, `tdd`, etc. Capture only; resolve this ticket properly (with Decision Shape questions) in its own session.

Follow-up decided 2026-07-31 (still while grilling Ticket 01): this metrics/audit log is explicitly OUTSIDE the daily product report (see Ticket 04's forward note) — it's an internal artifact. `db/knowledge.md` (already described in `CLAUDE.md` as "накопленные знания из подтверждённых сигналов") is the natural existing candidate home rather than inventing a new file — confirm this when the ticket is actually resolved rather than assuming it.

Forward note (2026-07-31, captured while grilling Ticket 02 - Signal Ontology, not resolved): every numeric threshold introduced while defining the 6 signal types (velocity quartile for Rising impulse, `persistence>=14d` for Persistent background, `source_spread>=3/4` for Synchronized story, more to come for One-source anomaly/Noise) needs to live in a versioned parameter reference, not hardcoded — value, description, and change history per parameter. Airat sharpened this on Synchronized story: it's not a flat table but a **classification hierarchy** — one level groups a cluster of related metrics with several values, another level holds the individual versioned parameters underneath. Recalibrating these thresholds from accumulated logs is explicitly a separate future module (his words: "какой-то уже machine learning или data science" review of what worked/didn't) — out of scope for now; this ticket only needs to guarantee the parameters and their history are recorded so that recalibration is possible later.

Forward note (2026-07-31, captured while grilling Ticket 03 - Quality Gate Contract, not resolved): Airat proposed a **diagnostic/support agent role** — separate from the reviewer/implementer roles already in the Working Decision above. When a run fails or behaves unexpectedly (collector error, missing data, an agent step that silently didn't run), this agent walks the logs, identifies which stage/step broke and why, and *proposes* a fix or a re-run. Explicit guardrail he insisted on: this agent must NOT auto-patch or rewrite code on its own — a failure might be a transient blip (network hiccup, server reboot) rather than an actual bug, and treating every failure as "needs a code patch" would cause unnecessary, possibly wrong changes. So the role is diagnose-and-propose, human (Airat) approves before any code changes. Tied to the same session's decision that every blocked/failed run must escalate to Airat, never fail silently (see Ticket 03's Working Decision once resolved) — this agent is naturally the thing that turns a bare escalation into a legible "here's what broke and why." Resolve this properly (roles, when it runs, what artifact it produces, how "propose a fix" is bounded) when this ticket is actually claimed.

Forward note (2026-07-31, captured while grilling Ticket 03 - Quality Gate Contract, not resolved): Airat flagged a cost/role split for the implementation stage — a cheaper, non-conversational, code-only LLM for pure coding/implementation steps, reserving the pricier conversational model (DeepSeek today) for interpretation/labeling and for the grilling-style dialogue itself. Also a standing meta-point for this whole grilling session: the existing prototype code (`analytics/news_pressure_cluster.py`, `scripts/news_pressure_regimen.py`) referenced throughout Ticket 03's questions is grounding material for concrete discussion, not a locked-in architecture — reimplementation at Ticket 08/implementation time is expected to diverge from current names/structures once the contract is settled. Resolve the actual role/model split (which steps qualify as "pure coding", which model, cost thresholds) when this ticket is claimed.

Forward note (2026-07-31, captured while grilling Ticket 10 - Market Coupling Model, not resolved): `design.md`'s own "Open Questions" left unresolved whether the first Auditor is a separate subagent or the main session in a review stance. Airat sharpened the reasoning behind this while discussing it: role independence is non-negotiable — an agent must never audit its own work (his example: a coder and a tester must not be the same role/context, or it "covers for itself"). He generalized this beyond agent roles into a project-wide architecture principle: **the interaction contract between modules/roles must stay fixed; only the logic inside a module changes.** In his words, the project is built from modules — you tune or rewrite what happens *inside* a module, you don't rewrite how modules *talk to each other*. Applied to this ticket's open question: whichever roles this ticket settles on (Runner/Auditor/Synthesizer or otherwise), (1) Runner and Auditor must be genuinely separate agent instances/contexts, never the same one wearing two hats, and (2) the artifact handoff format between them (already a task-list item above: "Define exact artifact handoff format") is the stable interface — it should be designed carefully once, then left alone, while each role's internal logic (which metrics the Runner tries, which checks the Auditor runs) stays free to evolve independently. See also [[idea-buffer]]'s 2026-07-31 entry on the same principle applied to code modules generally (stable interfaces, swappable internals), and the `codebase-design` skill (deep-module vocabulary) as a candidate tool for actually drawing the interface boundary once this ticket is claimed for real design work.

Forward note (2026-07-31, captured while grilling Ticket 04 - Report Prototype, not resolved): Airat reinforced the diagnostic/support-agent forward note above (from Ticket 03's grill) while discussing where a "run completed but nothing reportable" case sits relative to "a module actually broke." He restated it concretely: when a module breaks, the agent notices via logs, does NOT self-fix, and instead files a ticket for what he called "2nd-line support" — i.e. himself — to review the next day; only a repeated/persistent failure gets prioritized for an actual fix. He added one new point not previously captured: **any fix, at any level (ops/technical, metrics, or product), must follow the project's documented concept/spec — never a quick ad-hoc patch just to make something stop failing.** This generalizes beyond the diagnostic agent to the whole "no shortcuts under pressure" ethos already implicit in the project's other principles (registry-driven entities, no empty modules, stable interfaces) — see the parallel entry added to `idea-buffer.md` the same session. Resolve the concrete mechanics (what counts as "persistent," how the ticket is filed, what "the documented concept" means operationally) when this ticket is actually claimed.
