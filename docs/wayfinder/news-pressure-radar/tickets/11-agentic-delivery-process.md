# Ticket 11 - Agentic Delivery Process

Status: closed
Type: grilling
Labels: `wayfinder:grilling`
Claim: claimed by Claude (wayfinder session, 2026-07-31)
Blocks: none formally (meta-process ticket — informs Ticket 08 - OpenSpec Bridge and Ticket 09 - Repository Boundary, especially the champion/challenger shadow-run mechanics inherited from Ticket 06, but neither ticket's own `Blocked By` lists it as a hard dependency; corrected 2026-07-31 to match `map.md`'s Current Frontier, which already treated this as non-blocking)

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

Forward note (2026-07-31, captured while grilling Ticket 07 - Delivery Policy, not resolved): Ticket 07 introduced a black-box threshold function (`should_notify_needs_review`, parameter `K`, default `1`) deciding whether an all-noise day's severity-b `needs_review` clusters (Ticket 03) are important enough to warrant their own Telegram ping. `K` was deliberately given a concrete, non-stub starting value (Ticket 02's "no empty modules" principle) but explicitly flagged as provisional — Airat's own framing: **"заполнить это значение каким-то показателем, который мы выясним из эмпирики. из нашего прогона"** (fill this value from an indicator we work out empirically, from our own runs). This is the first concrete candidate for this ticket's still-unresolved "recalibrate thresholds from the system's own accumulated operational logs" mechanism — distinct from Ticket 12's one-time bootstrap from pre-existing historical archive data, since `K` can only be tuned from data the running system itself generates over time. Resolve the actual recalibration mechanism (cadence, what counts as enough history, who/what approves a change to `K`) when this ticket is claimed.

**Resolved 2026-07-31 (this ticket's own grilling session):** see `## Working Decision` below.

## Working Decision

Resolved 2026-07-31 (`/grilling` session, second-largest of the day by question count). Full raw Q&A is in `## Grilling Transcript` below; this section is the synthesis. Grounded throughout by a background research pass over `src/agent/agent.py`, `src/agent/experiments.py`, `db/knowledge.md`'s actual write behavior, `db/experiments.db`'s schema, and the existing systemd timer — facts, not assumptions.

**1. Three altitudes, not one role taxonomy.** "Agentic process" language had accumulated across three genuinely different things, easy to conflate: (a) **the meta-process of building MADPAC itself** — this ticket's real subject; (b) **Runner/Auditor/Synthesizer** (Ticket 10) — the production coupling-analysis cycle; (c) **Generator/Evaluator** (Ticket 06) — the production quality-evaluation cycle. This ticket only designs (a); it also settles one open mechanic each is missing (Runner/Auditor separation, below), but does not rename or merge (b)/(c).

**2. Generation vs. verification is the one boundary that decides "same context or separate context."** Applied consistently across all three altitudes: stages that synthesize (clarify → research → spec) stay one continuous agent context — there is no self-checking risk in moving from idea to spec, only in moving from writing code to accepting it. Stages that verify (implement → test/review) require a genuinely separate context, per Airat's own standing principle from Ticket 10 ("a coder and a tester must not be the same role, or it covers for itself"). This single rule resolves Ticket 10's previously-open Runner/Auditor question too: **Runner and Auditor must be separate agent contexts**, handing off findings via a compact artifact (see point 4), not a shared chat thread.

**3. `wayfinder` → `grilling` → `openspec` remains the unchanged, un-diluted core for all decision/spec work.** This is not one option among several — it is the axis the entire project has been built on through ten closed tickets, and it does not get diversified or replaced by anything discussed in this ticket. Every other skill discussed below is a **satellite that only activates once `openspec` has produced a real `tasks.md`** — during the planning phase (where the project still is), nothing else is invoked.

**4. Skill mapping for the post-`tasks.md` (coding) phase** — replaces the Starting Assumption's placeholder `grill-up` (confirmed dead: no such skill exists in `mattpocock/skills`) with concrete, already-installed skills rather than inventing new agent machinery:

| Stage | Skill | Notes |
|---|---|---|
| Spec (lightweight ticket / durable contract) | `to-spec` / `openspec-propose` | Weight depends on scope |
| Implementation | `implement` | Direct replacement for the dead `grill-up` placeholder |
| Test discipline | `tdd` | Stays in the *generation* context — red-green-refactor is one continuous authoring flow, not independent verification |
| Independent review | `code-review` (+ `simplify`, `security-review` as needed) | Separate context from implementer, per point 2 |
| Diagnostic/support agent (Ticket 03/04 forward note) | `diagnosing-bugs`, **restricted to Phases 1–4 only** (build feedback loop → reproduce/minimise → hypothesise → instrument) | The skill's own Phase 5 is "Fix + regression test" — this ticket's guardrail ("diagnose and propose, never auto-patch") is satisfied by stopping before Phase 5 and filing a ticket with the confirmed root cause instead. This mechanism also closes the separate "fix per the documented concept, never an ad-hoc patch" forward note: the approved fix re-enters through the normal `implement` flow against the module's actual spec, not as a bolt-on patch from the diagnostic agent itself. |
| Cross-context handoff | `handoff` | The concrete answer to Ticket 10's still-open "define exact artifact handoff format" — a compacting, redaction-aware handoff document, not a bespoke format invented here |
| Memory/spec fixation | project `memory/` system + `openspec-archive-change` / `openspec-sync-specs` | Already in active use throughout this whole session |

**5. Model split is temporal (build vs. operate), not per-call-site.** An earlier framing (a `pure_code`/`interpretive` registry tagging individual LLM call-sites) was floated and explicitly rejected by Airat in favor of a simpler split he stated directly: **Claude Code builds and extends the system, always — full cost accepted, because decision quality and code quality both matter more than API cost during construction** ("я поэтому специально пользуюсь дорогой моделью... чтобы продумывание этой части было очень качественно"). **DeepSeek only enters once a working part of the system actually exists**, taking over the two *routine, frequent, checks-against-what-already-exists* roles: the diagnostic/support agent (point 4) and Ticket 06's Evaluator (Generator→Evaluator→Airat-audit). Any further building, extending, or approving of fixes stays on Claude Code, even post-launch. MADPAC's own existing internal pipeline calls (Ouroboros SQL hypotheses, headline-sentiment labeling) were already on DeepSeek before this session and are unaffected — this decision is only about which model runs the *process* roles designed in this ticket.

**6. One unified feedback contour, not several separate mechanisms.** Confirmed explicitly by Airat as a project-wide pattern already implicit across Tickets 02/06/07: run → record metrics (Metric Card's `observed_distribution`: min/max/mean/median) → compare against target (`interpretation_bands`, kept separate from the raw distribution) → Decision Block reconciles and decides (keep/correct/replace, with its existing 3-distinct-attempts loop-prevention before escalating to Airat) → the decision and its history become input for the next iteration. Champion/challenger and K-style threshold recalibration are **two different entry points into this same contour**, not two separate systems:
   - **Champion/challenger** (Ticket 06's forward note on shadow-run mechanics) — for changes that alter the analysis itself (a clustering threshold, a new source). Tested via **(A) replay against Ticket 06's frozen 1-year snapshot** when the change can be honestly evaluated retrospectively (fast, deterministic), or **(B) a live parallel shadow-run** for changes with no historical equivalent (e.g. a brand-new source has no history to replay). Both write to `regimen_runs` (point 7) tagged `run_type='shadow'` rather than a new table.
   - **Threshold recalibration** (e.g. `K` from Ticket 07) — for delivery-side parameters that only change *whether something is surfaced*, not what gets computed. No shadow-run needed at all: after **N=30** real operational days (one operating month — a concrete provisional value, not a guess, per Ticket 02's "no empty modules" principle), a statistical read of the accumulated `regimen_runs` history feeds a proposed new value through the same Decision Block. Generalizes to any future pure delivery-side threshold, not just `K`.

**7. Metrics/audit-log artifact: extend `regimen_runs`, do not invent a new store.** An earlier lean toward `db/knowledge.md` is rejected — background research this session confirmed `db/knowledge.md` is fully **overwritten** each session (not appended), flat prose, no per-run granularity, a poor fit for an append-only audit log. `scripts/news_pressure_regimen.py`'s existing `regimen_runs` table (one row per run, already carrying `send_allowed`, `block_reason`, `article_count`, `cluster_count`, `quality_flag_count`) is the natural home — extended with the columns this ticket's mechanisms need (`K` value used, whether a diagnostic-agent ticket was filed, champion/challenger comparison result, `run_type`), rather than a new file, per the project's standing "don't multiply entities" principle (Tickets 02/06).

## Grilling Transcript

Full record of the `/grilling` session behind the Working Decision above, per the convention on `docs/agents/issue-tracker.md`. A background research agent ran in parallel with the opening questions, checking `src/agent/agent.py`/`experiments.py` (confirmed: today's Ouroboros loop is fully undifferentiated — one model generates, repairs, and evaluates its own hypothesis, nothing resembling diagnose-without-fixing exists yet), `db/knowledge.md` (overwritten each run, not appended), `db/experiments.db` (already has `confirmed`/`signal_score`/`finding` columns, but scoped to fine-tuning export, not a general audit log), model routing (none exists — everything hardcoded to `deepseek-chat`), and the existing systemd timer (`deploy/systemd/user/news-pressure-regimen.timer`, daily at midnight UTC).

**Q1 — Are Runner/Auditor/Synthesizer, Generator/Evaluator, and this ticket's own role list (wayfinder/researcher/spec writer/implementer/tester/reviewer/summarizer) one taxonomy to consolidate, or different things at different altitudes?**
Claude proposed three altitudes (meta-development process / production coupling-analysis cycle / production quality-evaluation cycle) and recommended keeping them distinct rather than merging into one naming scheme.
Airat: **"конечно. об этом мы и рассуждали и говорили подробно"** — confirmed without change.

**Q2 — Which meta-process stages need a separate agent context vs. can stay one continuous thread?**
Claude proposed drawing the line at generation vs. verification: wayfinder→researcher→spec-writer as one continuous thread (no self-checking risk), implementer→tester/reviewer as separate contexts (real self-checking risk, per Airat's own coder/tester principle from Ticket 10).
Airat asked to ground this in the project's actual installed skills rather than the abstract role names, and to keep `wayfinder`/`grilling`/`openspec` as the unchanged core — see Q3.

**Q3 — Concrete skill mapping for each stage.**
Claude inventoried the full locally-installed skill set (broader than the global skill list — `.agents/skills/` includes `implement`, `handoff`, `to-spec`, `diagnosing-bugs`, and more), checked two candidates' actual bodies before proposing them (not just their descriptions), and presented the mapping table now in Working Decision point 4.
Airat: **"давай. полагаюсь на твой профессионализм. абстракцию. и видение больших проектов."**

**Q4 — How to bound `diagnosing-bugs` so it never auto-patches, per Airat's hard requirement from Ticket 03?**
Claude read the skill's actual phase structure (6 phases, Fix starts at Phase 5) and proposed stopping at the end of Phase 4, filing a ticket instead of proceeding — which also subsumes the separate "fix per documented concept" forward note, since the approved fix then re-enters through normal `implement` against the real spec.
Airat confirmed via the next message without objection, then raised a broader concern (Q5).

**Q5 (Airat-initiated, not a pre-planned question) — Concern that fragmenting into many skills might dilute the `wayfinder`/`grilling`/`openspec` core that built the whole project so far.**
Airat: **"наши основные скиллы должны остаться wayfinder grill openspec мы на них почти весь проект до сегодня написали."** Claude built the explicit backbone-vs-satellite decision tree now in Working Decision point 3, showing every other skill only activates after `openspec` produces real tasks — nothing changes about the current planning phase.
Airat: **"да все так... сейчас основа наша wayfinder / grilling / openspec это наше ядро верно же?"** — confirmed explicitly as core.

**Q6 — Metrics/audit-log artifact: where does it live?**
Claude reported the research finding that `db/knowledge.md` (the earlier lean) is a poor fit (overwritten, not appended) and proposed extending the already-existing `regimen_runs` table instead of inventing a new store.
Airat: **"да."**

**Q7 — Cost/model split for pure-code vs. interpretive pipeline steps.**
Claude initially proposed a registry-driven call-site tagging mechanism (`pure_code`/`interpretive` classes). Airat redirected to a simpler framing: Claude Code (expensive) builds everything during development regardless of cost — **"мы уже сегодня потратили 100 долларов. но плодотворно"** — and DeepSeek only connects later, once some part of the system is actually working, as a "servicing model." Claude asked for clarification on scope (is this about the meta-dev process or the production pipeline's own calls?) and proposed that DeepSeek specifically take over the two already-designed *routine, frequent, checks-what-exists* roles (diagnostic agent, Ticket 06's Evaluator) once live, while Claude Code stays reserved for all further building/extending.
Airat: **"да."**

**Q8 — Champion/challenger shadow-run mechanics (Ticket 06's forward note).**
Claude proposed two testing modes depending on whether the change has historical equivalent — (A) replay against Ticket 06's frozen snapshot, (B) live parallel shadow-run for changes with no historical analogue (e.g. a brand-new source) — both logged into `regimen_runs` tagged `run_type='shadow'`.
Airat: **"да."**

**Q9 — K-threshold (and general delivery-side threshold) recalibration mechanism.**
Claude distinguished this from champion/challenger: `K` doesn't change what's computed, only whether it's surfaced, so no shadow-run is needed — a retrospective statistical read of accumulated `regimen_runs` history after N=30 real operational days, feeding the same Decision Block.
Airat connected this to a broader standing pattern he'd been building across the whole project — measure, compare to target, decide, record, next iteration reuses the recorded knowledge — and asked Claude to confirm the whole contour was coherent as one thing, not separate mechanisms. Claude laid out the full contour (Metric Card → Decision Block → champion/challenger and K-recalibration as two entry points into the same mechanism) in Working Decision point 6.
Airat: **"да. хорошо."**, then separately: **"да"** confirming `N=30`.

**Q10 — Runner/Auditor separation (Ticket 10's original open question, forwarded here).**
Claude applied the same generation/verification boundary from Q2 to this altitude: Runner and Auditor must be separate contexts, handoff via the `handoff` skill's artifact format from the Q3 table.
Airat: **"да."**

**Closing confirmation.** Claude summarized the remaining, now-mechanical Decision Shape items (idea-to-code trigger = arrival in `tasks.md`; spike acceptance = `code-review` passing both axes; review failure = iterate back through `implement`, no new mechanism; `grill-up` placeholder replaced per the Q3 table) and asked if anything was missing.
Airat restated the overall vision in his own words — finish dispelling the fog with `wayfinder`/`grilling`/`openspec`, then use the well-designed available skills "in good traditions" to write good code — and asked Claude to confirm this matched. Claude confirmed it matched exactly what had just been built, closing the ticket.
