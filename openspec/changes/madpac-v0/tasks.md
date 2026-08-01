# Tasks: MADPAC v0

Status: draft
Build order rationale: see `design.md`'s Migration Plan — capabilities with no unresolved calibration dependency first, capabilities whose spec flags a provisional value implemented against their registry default with the gap tracked, not blocked on.

## 1. Shared Registries And Infrastructure

- [ ] 1.1 Implement the versioned parameter/entity registry (hierarchical: family → individual value + description + change history), per `analysis`'s Versioned Parameter And Entity Registry requirement.
- [ ] 1.2 Implement the Metric Card schema (`id`/`version`, `purpose`, `computation`, `inputs`, `observed_distribution`, `interpretation_bands`, `window_applicability`, `llm_notes`), per `evaluation`'s Metric Card Registry requirement.
- [ ] 1.3 Extend `regimen_runs` (in `scripts/news_pressure_regimen.py`) with threshold-value-used, diagnostic-ticket-filed flag, champion/challenger result, and `run_type` columns, per `agentic-research-loop`'s Regimen Runs requirement.
- [ ] 1.4 Resolve the open question between `regimen_runs` and the SQLite experiment registry (`design.md` Open Questions) before building either further.
- [ ] 1.5 Resolve dataframe/runtime choice (e.g. Polars candidate) after checking existing repository dependencies — carried from the original pre-grill process design (2026-07-28), not a separately grilled ticket decision; revisit if it stops fitting.

## 2. Collection

- [ ] 2.1 Add `kind`/`route` fields to `config/news_pressure_sources.yaml` for all 14 active sources, per `collection`'s Active Source Set requirement.
- [ ] 2.2 Verify `rbc`'s RSS route and `svoboda.org`'s feed URL from the production collector host before wiring either into the regimen.
- [ ] 2.3 Implement per-source VPN routing (`route: vpn`) for `meduza.io`/`svoboda.org`; verify VPN reachability from wherever the collector actually runs before relying on it.
- [ ] 2.4 Implement the pluggable ingestion contract: one output shape `(url, title, published_date, source, lead_text?)` across all `kind` handlers.
- [ ] 2.5 Add the three-axis source categorization (alignment, foreign/country, legal-designation) to each active source's config entry.
- [ ] 2.6 Implement hourly RSS polling with URL + fuzzy-title dedup.
- [ ] 2.7 Confirm text-depth capture (`headline+lead` where available at no extra cost) matches `collection`'s Text Depth By Source requirement.

## 3. Analysis

- [ ] 3.1 Implement the 6 canonical signal-type classifiers (Main pressure, Rising impulse, Persistent background, Synchronized story, One-source anomaly, Noise/routine) against registry-held thresholds.
- [ ] 3.2 Implement the news-plane × market-plane cross-check as a shared function reused across signal types, not duplicated per type.
- [ ] 3.3 Implement state-transition logic (Rising impulse → Persistent background at 3 months; One-source anomaly → Noise/routine on unconfirmed follow-up; Noise/routine rescue on market confirmation).
- [ ] 3.4 Seed the instrument registry from the MOEX ISS API; set `active_in_pilot_v0` per Ticket 10's confirmed subset; confirm exact local symbols for oil/gas and financial sector indices and available FX series/frequency (`design.md` Open Questions).
- [ ] 3.5 Implement the D-3..D+3 (optional D+5) event-window mechanics as registry-driven config, not hardcoded ranges.
- [ ] 3.6 Implement the anomaly metric-vector pool (raw values stored per instrument/date, no hard pass/fail gate) against a starting candidate metric list.
- [ ] 3.7 Implement the market-coupling output row schema (`instrument_id, event_date, window, metric_vector, news_source_count, news_headline_count, news_sync_flag, news_peak_timing, relation_note, negative_case`).
- [ ] 3.8 **Tracked calibration gap, not a blocker:** velocity-quartile threshold — no cross-month time series exists yet; implement against a starting registry value and flag for recalibration once operational history accumulates.
- [ ] 3.9 **Tracked calibration gap, not a blocker:** `source_spread` thresholds — implement against the current registry defaults, but do not calibrate or validate them from the English `hf_news.db` archive; recalibrate from live 14-source Russian production history only, once enough has accumulated.
- [ ] 3.10 **Short confirm-or-adjust pass needed, not a full re-grill:** `persistence>=14d` sits near the real p95 boundary — confirm with Airat whether "top ~5%" or a broader framing (closer to p75=5 days) was intended, before treating either as final.

## 4. Quality Gate

- [ ] 4.1 Implement the 4 unconditional hard blockers (run failed, collector errors, no articles, no clusters) with whole-report blocking.
- [ ] 4.2 Implement the never-silent escalation rule: every block, hard or soft, notifies Airat with its specific reason.
- [ ] 4.3 Implement the per-cluster severity model: severity (a) `unsupported_number`/`thin_cluster` (annotate, keep); severity (b) `single_source_persistent`/`single_source_high_volume`/`llm_fallback_heuristic` (exclude into needs-review).
- [ ] 4.4 Implement the `llm_fallback_heuristic` flag specifically — currently a silent gap where heuristic-fallback interpretations are presented unflagged.
- [ ] 4.5 **Tracked calibration gap, not a blocker:** the 30%/50% report-level exclusion-share cap — not evaluated by Ticket 12; either run the `quality_flags()` historical simulation or make and record an explicit reasoned starting-value decision before treating this cap as active.

## 5. Report

- [ ] 5.1 Implement the canonical Markdown report shape and section ordering.
- [ ] 5.2 Build the canonical↔human-label dictionary as the single source of section header text.
- [ ] 5.3 Implement the Product-report vs. Technical/Ops-report mode split, triggered by `signal_cluster_count == 0` on an otherwise-successful run.
- [ ] 5.4 Implement the needs-review section (severity-b clusters) inside the product report.
- [ ] 5.5 Implement evidence links (`article.url` rendered as Markdown links per representative headline).
- [ ] 5.6 Implement the attributed watch-next list (per-cluster sentences tagged by originating signal type).
- [ ] 5.7 Implement the fact-based Executive Summary (concrete lead sentence + bullet-count nav list).

## 6. Delivery

- [ ] 6.1 Implement the three delivery surfaces (Telegram channel, email, local pull-only page) sharing the canonical report data.
- [ ] 6.2 Implement the single daily schedule triggering both push channels together.
- [ ] 6.3 Implement the 3-level metric-driven delivery-decision function (`send_allowed` → `signal_cluster_count` → `needs_review_count` vs. `K`).
- [ ] 6.4 Implement the dedicated needs-review Telegram ping for the all-noise-day gap case.
- [ ] 6.5 Implement the recipient-storage interface function (`get_recipient(channel)`) reading from a single config file.
- [ ] 6.6 Confirm text-only rendering on every surface (no charts/color at v0).
- [ ] 6.7 **Tracked calibration gap, not a blocker:** `K` (default `1`) is a provisional starting value — recalibrate via the Decision Block after N=30 real operational days.

## 7. Evaluation

- [ ] 7.1 Extract the frozen, versioned 1-year headline-only snapshot from `db/hf_news.db` (read-only, DuckDB `ATTACH ... TYPE sqlite`), shared with Ticket 12's calibration use.
- [ ] 7.2 Implement the independent sentiment index (per-headline LLM score against a versioned prompt, versioned aggregation formula) — never reusing `pressure_score_v2`.
- [ ] 7.3 Implement the pre-registered Track 1 statistical method (Spearman per lag × instrument, Benjamini-Hochberg FDR, block bootstrap) exactly as pre-registered, with one permitted verification run.
- [ ] 7.4 Populate the known-answer calibration-anchor registry (CB rate decisions, Rosstat releases, earnings, MOEX events) and validate the method against it before trusting it on unlabeled dates.
- [ ] 7.5 Implement the Generator → Evaluator → Airat-audit three-role loop with the four-state handoff protocol.
- [ ] 7.6 Implement the domain-relative historical-baseline check, reusing the existing Ouroboros SQL-hypothesis engine (`src/agent/agent.py`, `src/agent/experiments.py`).
- [ ] 7.7 Implement the Decision Block: trigger detection, mandatory purpose/algorithm/divergence reconciliation, label-vs-metric distinction, self-documenting ticket + DB record, 3-distinct-attempts loop prevention.
- [ ] 7.8 Implement champion/challenger shadow-run mechanics (historical replay and live shadow-run modes), writing to `regimen_runs` with `run_type='shadow'`.
- [ ] 7.9 Implement the weekly system-health record and the weekly bounded-choice audit cadence.

## 8. Agentic Research Loop

- [ ] 8.1 Write the Runner, Auditor, and Synthesizer role prompts, enforcing separate agent contexts for Runner vs. Auditor.
- [ ] 8.2 Wire the `handoff` skill as the Runner → Auditor artifact-transfer mechanism.
- [ ] 8.3 Confirm whether the four-state handoff protocol applies to the Runner → Auditor → Synthesizer chain, not only Generator → Evaluator → Audit (`design.md` Open Questions).
- [ ] 8.4 Implement the anti-loop hypothesis registry (fingerprint + `next_allowed_if` blocking).
- [ ] 8.5 Wire the satellite-skill activation gate: no skill besides `wayfinder`/`grilling`/`openspec` used for decisions before a real `tasks.md` exists; `diagnosing-bugs` capped at Phase 4.
- [ ] 8.6 Confirm the temporal model-selection split is actually applied in practice (build-time calls on the high-capability model; diagnostic/Evaluator calls on the cheaper model only once their target subsystem works).

## 9. Review And Close

- [ ] 9.1 Run `openspec validate --strict madpac-v0` and resolve any reported issues.
- [ ] 9.2 Give Airat a review pass over `proposal.md`, `design.md`, and all `specs/*/spec.md` before treating the compilation as final.
- [ ] 9.3 Close Ticket 08 (OpenSpec Bridge) once the review pass is accepted.
- [ ] 9.4 Hand off to Ticket 09's physical `git filter-repo` extraction, per its own sequencing and path list.
