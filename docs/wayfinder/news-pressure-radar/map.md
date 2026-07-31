# Wayfinder Map: MADPAC

Status: charted
Tracker: local markdown
Created: 2026-07-28
Owner: Airat + Logos
Project Codename: MADPAC

## Destination

Reach a handoff-ready OpenSpec for MADPAC: a reliable personal analytical loop that connects news pressure, public speech, market traces, experiment logs, and agentic research process into one practical project.

This map ends when the route from current spike to implementation spec is clear: product contract, signal model, quality gate, report shape, delivery policy, evaluation method, and repository boundary are decided.

## Notes

- Domain: `signal-mind`, branch `news-pressure-radar-spike`.
- Project name clarified on 2026-07-28: call the whole project `MADPAC`. Existing terms remain descriptive subdomains: `News Pressure Radar` for news-pressure collection/reporting and `News-Market Coupling` for market-trace research.
- Current system: 5 active sources, SQLite store, daily/weekly/monthly/history regimen, graph clustering, DeepSeek labeling, `pressure_score_v2`, Markdown reports, JSON run status, systemd timer.
- Current operating stance: analytical contour first, no public product or dashboard yet.
- Product direction clarified on 2026-07-28: the first MVP is for understanding the world through digital traces, not for trading recommendations. Market data is evidence of social/economic reaction, not a buy/sell target.
- Market universe clarified on 2026-07-28: start with Moscow Exchange indices, sector indices, and ruble FX pairs against USD, EUR, and CNY. Individual stocks are out of the pilot unless a later ticket proves they are needed.
- Meta-goal clarified on 2026-07-28: the project is also a training ground for a disciplined multi-agent process. The desired process is: clarify a foggy idea, write a technical/spec artifact, implement a spike, independently review quality, decide whether to accept or iterate, then preserve learnings.
- Delivery is downstream from analysis. Telegram publication remains disabled until explicitly approved.
- Use these skills in later sessions: `wayfinder`, `grilling`/`grill-me` (one question at a time — no `grill-up` skill exists in `mattpocock/skills`, confirmed against the upstream repo tree on 2026-07-31), `cultural-research-story` only if the work touches media/source research, and OpenSpec/spec-driven workflow once the map has enough decisions.
- Work one decision ticket at a time. Research tickets may run in parallel; grilling tickets require Airat in the loop.
- Known current status on 2026-07-28 UTC: daily run completed but `send_allowed=false` due to 1 quality flag; weekly has 7 quality flags; history has 1 quality flag.
- Standing preferences clarified 2026-07-31:
  - The coupling idea deliberately allows both causal directions — news pressure shaping market traces, and markets moving first with news tone/cadence (e.g. top-10 headlines/hour) shifting afterward. Direction and news-granularity are open questions, not a decision — they feed Ticket 01's "primary value" and Ticket 10's "news-first vs market-first vs both" question, not a standing assumption to build against yet.
  - Airat weighs functionality and the shape of the implementation approach above code polish at this stage — this is a planning-heavy effort, not a production-hardening one.
  - Working order is idea → plan → tools → implementation, in that order; don't let tooling setup or code get ahead of an unresolved decision ticket.
- **Session resume point (2026-07-31):** Tickets 01, 02, 03, and 05 closed — see each ticket's `## Working Decision` + `## Grilling Transcript` for the full record, not just the one-line gists above. Ticket 05 closed with a primary-source research pass first (`docs/wayfinder/news-pressure-radar/research/05-source-strategy-findings.md`) and grew the active source set from 5 to 14; it also expanded [Ticket 12](tickets/12-historical-cluster-calibration.md)'s scope with a forward note (empirical calibration now needs to cover the new 14-source RU set, not only the English-heavy `hf_news.db` archive, target depth ~1 year). This closing also revealed Tickets 04 and 06 were already formally unblocked (their dependencies had all closed) but the map hadn't surfaced them yet — fixed this session, see `## Current Frontier` for the full priority order and reasoning. Next session should claim [Ticket 10 - Market Coupling Model](tickets/10-market-coupling-model.md) first, then 04, then 06 — and check Tickets 07, 11 for forward-notes left mid-interview before re-deciding anything on them from scratch.

## Decisions So Far

- [Ticket 01 - Product Contract](tickets/01-product-contract.md) — solo/Airat-only, daily from day one, retrospective-only correlation framing (no directional forecasts), success measured by a backtested reference correlation rather than gut feel; report/quality-gate/metrics-core kept as three separate layers (Tickets 01+04 / 03 / 11).
- [Ticket 02 - Signal Ontology](tickets/02-signal-ontology.md) — 6 canonical signal types (Main pressure, Rising impulse, Persistent background, Synchronized story, One-source anomaly, Noise/routine) each grounded in existing code, sharing 4 cross-cutting principles: versioned parameter registry, no empty modules, state transitions over permanent labels, and a reused news-plane×market-plane cross-check.
- [Ticket 03 - Quality Gate Contract](tickets/03-quality-gate-contract.md) — hard blockers (run failed/collector errors/no articles/no clusters) stay unconditional; every block, hard or soft, must escalate to Airat, never silent; per-cluster severity replaces all-or-nothing report blocking (severity a = annotate & keep in main signal: `unsupported_number`, `thin_cluster`; severity b = exclude from main signal into a "needs review" section: `single_source_persistent`, `single_source_high_volume`, new `llm_fallback_heuristic`); report-level exclusion-share cap left provisional, spun out to Ticket 12.
- [Ticket 05 - Source Strategy](tickets/05-source-strategy.md) — active MVP set grown from 5 to 14 Russian sources (added RBC via an RSS route that bypasses its Qrator block, gazeta.ru, iz.ru, forbes.ru, banki.ru, novayagazeta.ru, meduza.io + svoboda.org via VPN routing for their Roskomnadzor blocks, agents.media); TASS dropped (explicit RSS-ban in its terms); legal designations (foreign-agent/undesirable-org) treated as descriptive labels, not gates, while the project stays private/non-redistributing — revisit at Ticket 07; RKN network blocks solved by per-source VPN routing, not exclusion; new non-numeric `category_spread` (state-only/independent-only/mixed) report label added; numeric source-diversity thresholds deferred to an expanded Ticket 12; scope stays Russian-only for now.

## Current Frontier

Priority order set 2026-07-31 (project-manager pass over the dependency graph, not just declared "Blocked By" fields — see rationale below each item):

1. [Ticket 10 - Market Coupling Model](tickets/10-market-coupling-model.md) — **resume here next session.** Already has substantial groundwork (MOEX ISS API confirmed live, instrument list pulled, D-3..D+3/D+5 window framing, a market-anomaly-first workflow proposed) — the remaining Decision Shape is narrower than a cold start. Ranked first because Ticket 06's own forward note makes Ticket 01's "backtested reference correlation" success criterion depend on a coupling model existing first — a real dependency the formal "Blocked By" fields don't capture.
2. [Ticket 04 - Report Prototype](tickets/04-report-prototype.md) — unblocked since 2026-07-31 (Product Contract + Signal Ontology both closed). Independent of Ticket 10; converts four closed decisions into a concrete, readable artifact — highest tangible-output-per-effort item on the board right now.
3. [Ticket 06 - Evaluation Harness](tickets/06-evaluation-harness.md) — formally unblocked since 2026-07-31 (Signal Ontology + Quality Gate Contract + Source Strategy all closed), but its forward note's "reference correlation" work is more meaningful once Ticket 10 has picked a coupling model — sequence after Ticket 10, not before.
4. [Ticket 11 - Agentic Delivery Process](tickets/11-agentic-delivery-process.md) — meta-process ticket; nothing else in the graph blocks on it, so it can wait behind the three above without cost.
5. [Ticket 12 - Historical Cluster Calibration Study](tickets/12-historical-cluster-calibration.md) — runs whenever, in parallel with any of the above; no live grilling needed. Scope expanded by Ticket 05 to also cover the new 14-source RU set (not just `hf_news.db`); does not gate Ticket 03/05/06's numbers today, only Ticket 08 eventually.

## Blocked Tickets

- [Ticket 07 - Delivery Policy](tickets/07-delivery-policy.md) depends on Product Contract, Quality Gate Contract, and Report Prototype (04 — still open).
- [Ticket 08 - OpenSpec Bridge](tickets/08-openspec-bridge.md) depends on Product Contract, Signal Ontology, Quality Gate Contract, Report Prototype, Delivery Policy, and Ticket 12 - Historical Cluster Calibration Study (numeric sign-off only — Tickets 02/03/05/10 themselves are not blocked by it).
- [Ticket 09 - Repository Boundary](tickets/09-repository-boundary.md) depends on OpenSpec Bridge and Evaluation Harness.

## Not Yet Specified

- Whether the system should later become a multi-user product, a private intelligence assistant, or a publishing workflow.
- Whether to add a dashboard, and what exact dashboard decisions become necessary after reports prove useful.
- Whether to use embeddings, lemmatization, or both as the next semantic clustering layer.
- Whether the final project should integrate with Obsidian, Telegram channel publishing, email reports, or all of them.
- Whether agent-written interpretations should be treated as prose summaries only or as structured claims that must pass stricter evidence checks.
- Whether the market coupling layer should use event-study statistics, simple association tables, supervised models, or all of them in stages.
- Which parts of the work should be handled by separate agents versus one main agent with explicit review phases.

## Out Of Scope

- Trading signals or investment advice. This project measures agenda pressure, not buy/sell decisions.
- Individual stock-picking in the first pilot. Sector and broad indices are the first market lens.
- Public automatic publishing before a delivery policy and quality gate are closed.
- Full article scraping as a first-class dependency. The current MVP works from headlines; full text can return only if a later decision proves it necessary.
- A polished SaaS dashboard. Dashboard work starts only after the report contract is useful for repeated use.
- Rewriting the existing spike from scratch. The map should preserve the working regimen and clarify the next decisions around it.

## Suggested Session Order

1. ~~Resolve [Ticket 01 - Product Contract](tickets/01-product-contract.md) with `grill-me`.~~ Closed 2026-07-31.
2. ~~Resolve [Ticket 02 - Signal Ontology](tickets/02-signal-ontology.md) with domain modeling.~~ Closed 2026-07-31.
3. ~~Resolve [Ticket 03 - Quality Gate Contract](tickets/03-quality-gate-contract.md) using current run failures as examples.~~ Closed 2026-07-31.
3a. (parallel, no live grilling required) Run [Ticket 12 - Historical Cluster Calibration Study](tickets/12-historical-cluster-calibration.md) once `db/hf_news.db` is confirmed reachable. Feed results back into Tickets 02, 03, 05, 10 as short revision addenda once ready — does not block step 4 or 5 from closing today.
4. ~~Resolve [Ticket 05 - Source Strategy](tickets/05-source-strategy.md) with research.~~ Closed 2026-07-31.
5. **← Next.** Resolve [Ticket 10 - Market Coupling Model](tickets/10-market-coupling-model.md) before any market-code implementation — reprioritized 2026-07-31 to run before Ticket 04/06 because Ticket 06's success-metric work depends on it.
6. Build [Ticket 04 - Report Prototype](tickets/04-report-prototype.md) as a concrete markdown/email/Telegram prototype — independent of Ticket 10, can also run first if a quick tangible win is preferred in the moment.
7. Resolve [Ticket 06 - Evaluation Harness](tickets/06-evaluation-harness.md) now that Signal Ontology, Quality Gate Contract, and Source Strategy are all closed and (once step 5 lands) a coupling model exists to backtest against.
8. Resolve [Ticket 11 - Agentic Delivery Process](tickets/11-agentic-delivery-process.md) so the project teaches the workflow, not only the market idea — nothing else blocks on this, so it can slot in whenever convenient.
9. Resolve [Ticket 07 - Delivery Policy](tickets/07-delivery-policy.md) once Report Prototype (step 6) is closed.
10. Turn the settled decisions into OpenSpec through [Ticket 08 - OpenSpec Bridge](tickets/08-openspec-bridge.md), then [Ticket 09 - Repository Boundary](tickets/09-repository-boundary.md).
