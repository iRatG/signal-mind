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
  - The coupling idea originally left both causal directions open — news pressure shaping market traces, and markets moving first with news tone/cadence (e.g. top-10 headlines/hour) shifting afterward. Resolved 2026-07-31 on [Ticket 10](tickets/10-market-coupling-model.md): v0 runs market-first only (resource asymmetry — deep free MOEX history vs. thin RU news history), news-first/bidirectional deferred to stage 2.
  - Airat weighs functionality and the shape of the implementation approach above code polish at this stage — this is a planning-heavy effort, not a production-hardening one.
  - Working order is idea → plan → tools → implementation, in that order; don't let tooling setup or code get ahead of an unresolved decision ticket.
- **Session resume point (2026-07-31, second update):** Tickets 01, 02, 03, 05, and now 10 closed — see each ticket's `## Working Decision` + `## Grilling Transcript` for the full record, not just the one-line gists above. Ticket 10 was grilled from a clean slate this session, deliberately ignoring a pre-existing 2026-07-28 OpenSpec draft (`openspec/changes/news-market-coupling-agentic-loop/`) as a source of default answers — that draft is being reconciled against the fresh decision as a follow-up in the same session (check its `design.md`/`tasks.md`/mini-spec/runner-brief for an updated-vs-superseded marking before trusting their older content). A new `docs/wayfinder/news-pressure-radar/idea-buffer.md` file now holds cross-ticket architectural ideas that surfaced mid-grill but don't belong to one ticket (registry-driven entities, stable module interfaces/role independence) — check it before assuming those principles live only on the ticket that first mentioned them. Ticket 10 also left two items for other tickets to pick up: an Editor/rendering role and Telegram-vs-email output split (→ Ticket 04, forward note added) and whether the Auditor is a separate agent from the Runner (→ Ticket 11, forward note added). Next session should claim [Ticket 04 - Report Prototype](tickets/04-report-prototype.md) first, then 06, then 11 — see `## Current Frontier` for the full reasoning.

## Decisions So Far

- [Ticket 01 - Product Contract](tickets/01-product-contract.md) — solo/Airat-only, daily from day one, retrospective-only correlation framing (no directional forecasts), success measured by a backtested reference correlation rather than gut feel; report/quality-gate/metrics-core kept as three separate layers (Tickets 01+04 / 03 / 11).
- [Ticket 02 - Signal Ontology](tickets/02-signal-ontology.md) — 6 canonical signal types (Main pressure, Rising impulse, Persistent background, Synchronized story, One-source anomaly, Noise/routine) each grounded in existing code, sharing 4 cross-cutting principles: versioned parameter registry, no empty modules, state transitions over permanent labels, and a reused news-plane×market-plane cross-check.
- [Ticket 03 - Quality Gate Contract](tickets/03-quality-gate-contract.md) — hard blockers (run failed/collector errors/no articles/no clusters) stay unconditional; every block, hard or soft, must escalate to Airat, never silent; per-cluster severity replaces all-or-nothing report blocking (severity a = annotate & keep in main signal: `unsupported_number`, `thin_cluster`; severity b = exclude from main signal into a "needs review" section: `single_source_persistent`, `single_source_high_volume`, new `llm_fallback_heuristic`); report-level exclusion-share cap left provisional, spun out to Ticket 12.
- [Ticket 05 - Source Strategy](tickets/05-source-strategy.md) — active MVP set grown from 5 to 14 Russian sources (added RBC via an RSS route that bypasses its Qrator block, gazeta.ru, iz.ru, forbes.ru, banki.ru, novayagazeta.ru, meduza.io + svoboda.org via VPN routing for their Roskomnadzor blocks, agents.media); TASS dropped (explicit RSS-ban in its terms); legal designations (foreign-agent/undesirable-org) treated as descriptive labels, not gates, while the project stays private/non-redistributing — revisit at Ticket 07; RKN network blocks solved by per-source VPN routing, not exclusion; new non-numeric `category_spread` (state-only/independent-only/mixed) report label added; numeric source-diversity thresholds deferred to an expanded Ticket 12; scope stays Russian-only for now.
- [Ticket 10 - Market Coupling Model](tickets/10-market-coupling-model.md) — grilled from a clean slate (explicitly ignoring the pre-existing 2026-07-28 OpenSpec draft as a source of default answers). Market-first for v0 (anomaly in market data → look back at news), not news-first/bidirectional — deferred to stage 2 alongside pairwise index/currency signals; reasoning is a real resource asymmetry (deep, free MOEX history vs. thin RU news history), not a data ceiling — confirmed `ru_news_archive_loader.py` can targeted-backfill arbitrary historical dates for 3-4 of 14 sources today. Instrument universe and event windows (`D-3..D+3`, optional `D+5`) are registry/config-driven, never hardcoded in code — a new project-wide principle extending Ticket 02's parameter registry from thresholds to entities (see `idea-buffer.md`). Anomaly definition uses a pool of several metrics as a raw vector, not one fixed rule or hard gate. News-side "meaningfulness" stays descriptive (source count, sync flag) at v0, not a numeric cutoff — negative (no-news) cases are kept, not discarded. Surfaced two items routed elsewhere rather than decided here: whether the Auditor is a separate agent from the Runner (→ Ticket 11, with a new role-independence/stable-interface principle), and a news-Editor rendering role for Telegram/email output (→ Ticket 04).

## Current Frontier

Priority order refreshed 2026-07-31 after Ticket 10 closed:

1. [Ticket 04 - Report Prototype](tickets/04-report-prototype.md) — **resume here next session.** Unblocked since Product Contract + Signal Ontology both closed; independent of Ticket 10. Converts closed decisions into a concrete, readable artifact — highest tangible-output-per-effort item on the board. Now also carries a forward note from Ticket 10's grilling: a distinct Editor role (truthful, no-invention rendering; Telegram-short-with-graphics vs. email-detailed-HTML-with-graphics split) that this ticket needs to reconcile with its existing 3-section daily-structure forward note.
2. [Ticket 06 - Evaluation Harness](tickets/06-evaluation-harness.md) — formally unblocked (Signal Ontology + Quality Gate Contract + Source Strategy all closed); its forward note's "reference correlation" work can now actually reference Ticket 10's closed market-first coupling model instead of waiting on an undecided one.
3. [Ticket 11 - Agentic Delivery Process](tickets/11-agentic-delivery-process.md) — meta-process ticket; nothing else in the graph blocks on it. Picked up a new forward note from Ticket 10: whether the Auditor is a separate agent from the Runner, framed around a general role-independence/stable-interface principle (see `idea-buffer.md`).
4. [Ticket 12 - Historical Cluster Calibration Study](tickets/12-historical-cluster-calibration.md) — runs whenever, in parallel with any of the above; no live grilling needed. Scope covers both the English `hf_news.db` archive and the new 14-source RU set; does not gate Ticket 03/05/06/10's numbers today, only Ticket 08 eventually.

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
5. ~~Resolve [Ticket 10 - Market Coupling Model](tickets/10-market-coupling-model.md) before any market-code implementation.~~ Closed 2026-07-31 (grilled from a clean slate; market-first for v0).
6. **← Next.** Build [Ticket 04 - Report Prototype](tickets/04-report-prototype.md) as a concrete markdown/email/Telegram prototype — also now carries a forward note on the Editor/rendering role from Ticket 10.
7. Resolve [Ticket 06 - Evaluation Harness](tickets/06-evaluation-harness.md) now that Signal Ontology, Quality Gate Contract, Source Strategy, and Market Coupling Model are all closed — a coupling model now exists to backtest against.
8. Resolve [Ticket 11 - Agentic Delivery Process](tickets/11-agentic-delivery-process.md) so the project teaches the workflow, not only the market idea — nothing else blocks on this, so it can slot in whenever convenient.
9. Resolve [Ticket 07 - Delivery Policy](tickets/07-delivery-policy.md) once Report Prototype (step 6) is closed.
10. Turn the settled decisions into OpenSpec through [Ticket 08 - OpenSpec Bridge](tickets/08-openspec-bridge.md), then [Ticket 09 - Repository Boundary](tickets/09-repository-boundary.md).
