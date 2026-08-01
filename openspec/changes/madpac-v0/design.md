# Design: MADPAC v0

Project codename: MADPAC. See `proposal.md` for motivation and scope; see `specs/*/spec.md` for the per-capability behavior contracts this design implements.

## Context

The existing News Pressure Radar spike already has a data-oriented pipeline: 5-source collection, SQLite storage, graph clustering (`analytics/news_pressure_cluster.py`), `pressure_score_v2`, daily/weekly/monthly/history reports, quality flags, and a JSON run status (`scripts/news_pressure_regimen.py`). Per the standing project rule established during Ticket 04's grilling, this existing code is a fact-source for what data already exists — never a default shape for how the spec should be structured. Where this design or a spec delta conflicts with the current spike's actual behavior, the spec wins; the spike gets updated to match during implementation.

MADPAC v0 extends this from a single-source-of-truth pipeline into the seven capabilities in `specs/`: 14-source `collection`, 6-type `analysis` plus market coupling, per-cluster `quality-gate`, a canonical `report` shape, 3-surface `delivery`, a two-track `evaluation` harness, and the `agentic-research-loop` process both roles above were designed under.

## Goals / Non-Goals

**Goals:**

- One coherent v0 architecture spanning collection through delivery, where every cross-capability dependency (e.g. quality-gate flags feeding report sections feeding delivery levels) is traceable to a specific requirement in a specific spec file.
- Every numeric threshold and reference entity lives in a versioned registry, with its current calibration status (final vs. provisional) stated explicitly, never silently assumed.
- A build order that lets implementation start on capabilities with no unresolved calibration dependency, while capabilities blocked on real data (velocity, exclusion caps, `source_spread`) are implemented against their provisional defaults with the gap flagged, not silently worked around.

**Non-Goals (v0):**

- Public or multi-user delivery — every surface stays private, single-recipient (Airat).
- Charts, color, or any visual styling on any delivery surface.
- Real-time/intraday alerting — the system runs on a single daily schedule.
- Trading recommendations or directional forecasts — every signal type uses retrospective/historical-strength language only.
- Individual stock-picking — the pilot instrument universe is indices and FX pairs only.
- A dedicated English-language collection/NLP pipeline — scope stays Russian-only.
- Full article text as a first-class dependency — headline/lead is the default depth; full text is fetched only for a small ambiguous-anomaly candidate set.
- Physical repository extraction (`github.com/iRatG/madpac`) — that is Ticket 09's own execution step, sequenced after this proposal is reviewed, not part of this design.

## Decisions

### Registry-driven thresholds and entities, not hardcoded literals

Every numeric threshold (`analysis`'s signal-type cutoffs, `delivery`'s `K`) and every reference entity (the `analysis` instrument universe) lives in a versioned registry table, never as a literal in application code. **Alternative considered and rejected:** hardcoding v0 constants directly in code with a comment to revisit later — rejected because it repeats a documented anti-pattern (`db/forbidden_patterns.md`'s spirit) and makes future recalibration a code change instead of a data edit. **Rationale:** Ticket 02 and Ticket 10 both independently converged on this from different angles (thresholds, then entities), and Ticket 06 extended it a third time to metrics themselves (Metric Cards) — a threshold consistently pulled at from every direction is a real structural requirement, not a one-off preference.

### Per-cluster severity, not whole-report quality gating

`quality-gate` replaces a single summed flag-count blocking the whole report with two severity tiers evaluated per cluster (annotate-and-keep vs. exclude-into-review). **Alternative considered and rejected:** keep the original all-or-nothing model and just retune its threshold — rejected because it was already failing in practice (documented in `map.md`'s "known current status" note: single flags were blocking entire reports). **Rationale:** separating "this claim needs a caveat" from "this cluster shouldn't be trusted yet" lets the system keep sending useful reports on noisy days instead of going dark.

### Market-first coupling for v0, not news-first or bidirectional

`analysis`'s market-coupling model starts from a market anomaly and looks at surrounding news, never the reverse, for v0. **Alternative considered and rejected:** bidirectional analysis from day one — rejected due to a genuine resource asymmetry (deep, free MOEX history via the ISS API vs. a Russian news archive that, even after Ticket 12's calibration, covers only 9 months across 3 of 14 production sources). **Rationale:** market-first needs only targeted backfill around a handful of anomaly dates; news-first would need a full continuous multi-source historical run before any clustering signal is meaningful, which the news side cannot yet supply.

### Metric Card + Decision Block as the one calibration mechanism

`evaluation`'s Metric Card registry and Decision Block are the single mechanism behind every calibration decision project-wide: classification-mismatch calibration, persistent quality-gate blocks, champion/challenger promotion, and delivery-threshold recalibration all route through it (`agentic-research-loop`'s Unified Feedback Contour requirement makes this explicit). **Alternative considered and rejected:** let each capability invent its own local calibration logic (e.g. `delivery`'s `K` recalibrating independently of `analysis`'s threshold recalibration) — rejected because Airat identified, mid-grill, that these were already the same measure→compare→decide→record pattern appearing repeatedly across tickets; unifying them avoids three subtly-different reimplementations drifting apart over time.

### Three role altitudes, generation/verification as the one context-separation rule

`agentic-research-loop` keeps the meta-development process, the Runner/Auditor/Synthesizer cycle, and the Generator/Evaluator cycle as three distinct altitudes, but applies one shared rule across all three: generation stages may share an agent context, verification stages must run in a separate one. **Alternative considered and rejected:** define context-separation rules independently per altitude — rejected because it was exactly the same rule ("a checker must never check its own work," first stated by Airat in Ticket 10) appearing three times; one rule applied consistently is easier to audit than three parallel ones that could quietly diverge.

### Temporal, not per-call-site, model selection

A high-capability model builds and extends the system for as long as construction continues; a cheaper model only takes over already-designed, routine, check-against-what-exists roles (diagnostic agent, Evaluator) once that part of the system works. **Alternative considered and rejected:** classify individual call-sites as `pure_code` vs. `interpretive` and route by that tag — this was Claude's first proposal in Ticket 11 and Airat explicitly rejected it as more complex than needed. **Rationale:** the real cost/quality tradeoff tracks *when* in the system's life a call happens (building vs. operating), not *what kind* of call it is.

## Risks / Trade-offs

- **[Risk]** `source_spread` cannot be calibrated from the English `hf_news.db` archive at all (56% of it funnels through one re-syndication domain with no recoverable outlet identity) → **[Mitigation]** `analysis`'s spec explicitly forbids using that corpus for this threshold and requires recalibration from live 14-source Russian production history instead; the provisional Russian-archive number (3-of-14 sources) is documented as not yet representative.
- **[Risk]** `persistence>=14d`, velocity quartiles, and the 30%/50% quality-gate exclusion caps are either near an extreme percentile or entirely uncalibrated → **[Mitigation]** each is implemented against its current registry default with the calibration gap stated explicitly in the relevant spec requirement, so a wrong number is traceable to a known-open item rather than mistaken for a validated one.
- **[Risk]** `rbc`'s RSS route and `svoboda.org`'s feed URL are unconfirmed from the production collector host → **[Mitigation]** `collection`'s spec keeps these two sources flagged pending verification rather than silently marking the whole 14-source set as equally solid.
- **[Risk]** The VPN capability that routes `meduza.io`/`svoboda.org` around Roskomnadzor blocks is maintained in a separate checkout (`c:\project\signal_mind\vpn\`), unverified reachability from a future MADPAC-only repository → **[Mitigation]** flagged in Ticket 05 and carried here; implementation must verify reachability before wiring VPN routing into the extracted repo, not assume it.
- **[Risk]** `regimen_runs`' relationship to the `agentic-research-loop` capability's own SQLite experiment registry is undefined — two audit-log-shaped stores could drift apart → **[Mitigation]** flagged as an Open Question below rather than guessed at; must be resolved before both are implemented, not after.
- **[Trade-off]** Choosing one large `madpac-v0` change over 7 independently-shippable smaller changes means this proposal is heavier to review in one pass → accepted deliberately (see the scope decision recorded in `proposal.md`'s revision note) because the capabilities are tightly cross-referential for a from-scratch v0 (report depends on quality-gate fields, delivery depends on report and quality-gate fields, evaluation depends on the coupling model) and splitting them risked exactly the kind of drift this compilation step exists to prevent.

## Migration Plan

This proposal ships no code; "migration" here means the sequencing of what happens once it is reviewed:

1. Review this proposal, its specs, and this design with Airat (Ticket 08's own closing step).
2. Implement via `tasks.md`, in the dependency order it lays out — capabilities with no unresolved calibration dependency (`collection`, the structural half of `quality-gate`, `report`, `agentic-research-loop`'s process scaffolding) can start immediately; capabilities whose spec explicitly flags a provisional value (`analysis`'s velocity/`source_spread`, `delivery`'s `K`) are implemented against their current registry defaults, with the calibration gap tracked as a live follow-up, not a blocker.
3. Execute Ticket 09's physical `git filter-repo` extraction into `github.com/iRatG/madpac` now that Ticket 12's snapshot work and this compilation are both done — Ticket 09's own Working Decision explicitly downgraded proven operational stability from a pre-extraction gate to ongoing post-move work, so extraction does not wait on implementation stabilizing first. Extraction follows the path list and sequencing already decided on that ticket and in `map.md`'s Next Session Plan. This proposal's `openspec/changes/madpac-v0/` directory is expected to migrate whole, with git history, alongside the code and docs paths.
4. Archive this change (`openspec archive`) once implemented, so `openspec/specs/` gains its first real main-spec baseline for future changes to diff against.

## Open Questions

- What is the relationship between `regimen_runs` (the production audit log, extended per `agentic-research-loop`'s requirement) and the SQLite experiment registry that same capability's "Experiment Registry Supports Process Analysis" requirement calls for? Both are structured, queryable, audit-shaped stores; whether they are the same store, one feeds the other, or they serve genuinely different queries was not resolved during Ticket 11's grilling and needs a short decision before both are implemented.
- Has the four-state handoff protocol (`taken → in_progress → done → handed_off`) been applied to the Runner → Auditor → Synthesizer chain, or only to Generator → Evaluator → Audit? Ticket 11's own notes flag this as not explicitly re-confirmed for the coupling-analysis cycle.
- Which exact local symbols/names correspond to the oil/gas and financial sector indices in the `active_in_pilot_v0` instrument set, and which FX series exist locally at what frequency? Left for implementation-time confirmation against the MOEX ISS API, per Ticket 10.
- What happens to the original `docs/wayfinder/news-pressure-radar/`, `analytics/`, and `data/news_pressure/` paths inside `signal-mind` after Ticket 09's extraction is verified — kept as a historical artifact, or removed? Ticket 09's own Working Decision does not say; flagged there for Airat, not assumed either way here.
