# Ticket 06 - Evaluation Harness

Status: closed
Type: task
Labels: `wayfinder:task`
Claim: claimed by Claude (wayfinder session, 2026-07-31)
Blocked By: Ticket 02 - Signal Ontology, Ticket 03 - Quality Gate Contract, Ticket 05 - Source Strategy (all three closed 2026-07-31 — this ticket is now formally unblocked, though its forward note ties the "reference correlation" work to Ticket 10 closing first — see below)
Blocks: Ticket 08 - OpenSpec Bridge, Ticket 09 - Repository Boundary

## Question

How will we know the radar is getting better rather than just producing plausible prose?

## Why This Matters

LLM summaries can sound confident while hiding weak evidence. The project needs an evaluation layer that judges signal detection, report usefulness, and false positives across daily and longer windows.

## Task Scope

Define and, if needed, prototype:

- a small labeled set of past reports;
- review rubric for main/rising/persistent/anomaly/noise classifications;
- metrics for quality flags and delivery blocks;
- regression checks for cluster stability;
- review notes format that can feed future improvements.

## Decision Shape

The ticket resolves when there is a concrete evaluation harness spec with enough detail for implementation.

## Forward Note (2026-07-31, captured while grilling Ticket 01, not resolved)

Airat's "success after 30 days" answer on Ticket 01 depends on this ticket delivering more than a labeled-report rubric: he wants at least one statistically backtested reference correlation (an "эталон") between a synthesized news-tonality index and a real market/composite signal at a defined lag, validated against historical headlines pulled from as far back as the start of the year. That reference correlation is meant to become the benchmark other candidate signals get checked against — an objective replacement for gut-feel judgment of "is this working." Whatever historical headline backfill this needs should check `db/hf_news.db` first (already described in `CLAUDE.md` as the 1M+-article master news source) before treating it as a new data-gathering task.

Also carries Ticket 11's expectation that the metrics-ensemble itself may be window-specific (some metrics only hold at D+1, others at D+3) — the harness should be able to evaluate a metric per-window, not just pass/fail overall.

Forward note (2026-07-31, captured while grilling Ticket 02 - Signal Ontology, not resolved): Airat named a concrete future use for the versioned parameter registry being introduced across Ticket 02's signal types (see Ticket 11's forward note) — scenario/stress-test analysis: replay historical data through different threshold configurations and observe how indices actually reacted under each. This is this ticket's territory, not a Ticket 02 concern; resolve it here once the parameter registry exists.

## Working Decision — Track 1 (Statistical Backtest / "Эталон"), resolved 2026-07-31

The harness splits into two independent tracks, each answering a different question and evaluated on its own cycle/metrics — deliberately not blended into one process, because "is the news↔market hypothesis statistically real" and "is the classification pipeline producing trustworthy, well-written output" are different failure modes (the second is the one this ticket's own title question — "plausible prose" — is actually about):

- **Track 1 — Statistical Backtest ("Эталон").** Resolved this session, in full below.
- **Track 2 — Qualitative Rubric Review** (labeled report set, per-signal-type rubric, quality-gate/delivery-block metrics, cluster-stability regression checks, review-notes format). Still open — resume here next.

### Data source: frozen, portable headline snapshot

- A live cross-machine dependency is explicitly rejected. `db/hf_news.db` (confirmed this session: a 10.8 GB SQLite file, physically present and reachable at `c:\project\signal_mind\db\hf_news.db` — a separate, complete, live git checkout of this same repo, `origin` = `github.com/iRatG/signal-mind.git`, branch `master`, distinct from this checkout at `c:\Users\arate\signal-mind`) is read-only, machine-specific, and not something a 24/7 production service can depend on living.
- Instead: extract once, via a read-only DuckDB `ATTACH ... (TYPE SQLITE, READ_ONLY)` cross-engine read, a **versioned, portable snapshot** containing only: headline text, date, source, language — no full article text (consistent with Ticket 10's existing "headline by default" text-depth decision). Depth: **1 year**, matching the depth chosen for the MOEX market-side pilot (an explicit pilot-scope choice, not a technical ceiling — MOEX ISS API itself supports much deeper free history per Ticket 10's research).
- Dual purpose, one extraction: (a) input to Ticket 12's historical calibration coefficients, (b) training/backtest set for this ticket's эталон. Sharing the extraction script between the two tickets is expected but not designed here — implementation detail for whoever builds it.
- The resulting snapshot is what production actually ships with/reads from — decouples 24/7 stability from any single dev machine.

### Sentiment index: built fresh, not reused

- Explicitly **not** a reuse or adaptation of the existing `pressure_score_v2` (cluster/volume-based pressure metric in `analytics/news_pressure_cluster.py`). Reusing it would make the эталон tautological — a metric correlated against a signal derived from itself proves nothing.
- Built as an independent sentiment index: per-headline sentiment score computed by an LLM (DeepSeek — same provider the project already uses for cluster labeling) against a **fixed, versioned prompt template**, aggregated to a single daily value via a **versioned aggregation formula**. Both the prompt template and aggregation formula are treated as a formalized, deterministic, reproducible algorithm — LLM involvement in computing the raw score does not make the metric ad hoc; it just means one step of the fixed procedure happens to be an LLM call, not a lookup table.

### New project-wide mechanism: Metric Card registry

Extends Ticket 02's "versioned parameter registry for thresholds" and the idea-buffer's "registry-driven entities" principle to **metrics themselves**. Every metric — this эталон correlation, and (whenever built/retrofitted, not forced now) Ticket 10's anomaly-pool metrics and Ticket 03's quality flags — is a registry object with:

| Field | Meaning |
|---|---|
| `id` / `version` | registry identifier, version (metrics evolve) |
| `purpose` | why the metric exists, what question it answers |
| `computation` | the formalized, deterministic algorithm (prompt version + formula if LLM-involved) — never implicit in code |
| `inputs` | what data it needs |
| `observed_distribution` | descriptive only: median, mean, min, max, derived empirically from calibration on the 1-year snapshot |
| `interpretation_bands` | normative only, versioned **separately** from `observed_distribution`: which value ranges count as good/neutral/bad — a judgment call, revisable independent of the raw empirical distribution |
| `window_applicability` | which D-N/D+N windows the metric is valid for (resolves this ticket's own forward note about window-specific metrics) |
| `llm_notes` | natural-language interpretation guidance for an LLM consumer, judging contextual appropriateness — **never** authorizes overriding Ticket 03's hard blockers; soft judgment only |

Retrofitting Tickets 03/10's already-decided metrics into this schema is **not required now**. If retrofitting later surfaces a real conceptual conflict with what those tickets already decided, that gets raised and discussed explicitly — never silently resolved either by forcing the old metric into the new shape or by quietly leaving the conflict unaddressed.

### Statistical method for the эталон correlation

Locked and documented **before** running any calibration pass, deliberately — this is a pre-registration discipline (fix the method, then run it once, then report results/reasoning) chosen specifically to prevent p-hacking/tuning the test until it produces a flattering number, which would gut the entire "эталон instead of gut-feel" premise from Ticket 01.

- **Lag grid:** reuse Ticket 10's existing window grid (D-3..D+3, optional D+5) — no new grid invented.
- **Correlation coefficient:** Spearman, tested per lag × per instrument (the registry-driven MOEX instrument universe from Ticket 10 — 10+ sector indices, several FX pairs), not one blended market-wide number.
- **Multiple-comparisons control:** Benjamini-Hochberg FDR across all lag × instrument tests (tens to 100+ simultaneous tests) — chosen over Bonferroni, which is too conservative here and would likely suppress a real signal given the tests are not independent (adjacent lags/instruments correlate with each other).
- **Significance/CI method:** block bootstrap (moving blocks of ~5-10 trading days), not naive parametric p-values — corrects for the fact that a daily news-sentiment series is very likely autocorrelated day-to-day (news flow is inertial), unlike market returns which are close to serially uncorrelated; naive parametric tests would overstate significance by treating each day as an independent observation when it effectively isn't.
- **One verification run** against the 1-year snapshot to confirm the procedure runs and produces sane numbers — not an opportunity to revise the method based on the result.
- **Structural link to Ticket 10:** this general, full-sample эталон correlation is not just a standalone number — it becomes the **baseline/null** that Ticket 10's anomaly-triggered event-window signals get checked against ("is the news-market alignment in this specific anomaly window stronger than the year-round background level"). This gives Ticket 06 and Ticket 10 one shared statistical foundation instead of two independently invented, potentially incompatible procedures.

### Known-answer calibration anchors (registry, not a single example)

Airat surfaced a specific known-certain relationship (CB key-rate decisions → RUB/USD volatility in the week before and around the decision) as a candidate calibration anchor, then generalized it himself to a class: known, scheduled economic-calendar events are all candidate anchors — CB rate-decision calendar, Rosstat statistical-release calendar, quarterly bank/large-company earnings and audit reports, MOEX corporate/economic event announcements.

- **Mechanism (decided here):** these known events become a **registry of "known-answer test" checkpoints** — before trusting the Spearman + FDR + block-bootstrap method (above) on the full unlabeled year, first check it correctly recovers the expected relationship at each known-anchor date. If the method can't detect a relationship we already know is real, the method itself is broken and must be fixed before it's trusted on anything novel/unknown.
- Once validated, each known anchor's actual measured coefficients become a distinguished, high-confidence reference point inside the relevant Metric Card's `interpretation_bands` — not folded anonymously into the general population statistics, and not treated as the sole source of every threshold (a single event *type* — e.g. only CB-rate reactions — must not be extrapolated wholesale to unrelated topics without separately checking it generalizes).
- **Populating** the actual calendar (scraping/loading real CB/Rosstat/MOEX event dates) is implementation work for later, not decided here.
- A related, adjacent proposal surfaced mid-discussion — adding MOEX itself as a **news** source (its own corporate/economic announcements, distinct from its existing role as the Ticket 10 market-data API) — is recorded as a forward note on the already-closed [Ticket 05 - Source Strategy](05-source-strategy.md), not decided in this ticket. A further extension (a RAG layer over public quarterly audit reports/financial disclosures) was explicitly deferred by Airat to a later version and recorded in `idea-buffer.md`.

## Grilling Transcript — Track 1

**Q (Claude):** Should the harness be architected as two separate tracks — a statistical backtest (эталон) and a qualitative rubric review — each with its own success metrics, rather than one blended process?

**A (Airat):** [Redirected to a factual point before answering directly] "мы в одной ветке проекта signal-mind, он расположен тут — c:\project\signal_mind\... вообще весь проект тут описан... то есть база данных физически находится в c:\project\signal_mind\db\ в файлах hf_news.db (10 гигабайт). все что можно подсоединиться и подать перенаправить для единовременного эксперимента забора данных только заголовки как пока договорились. можно и в нашу базу данных? это возможно?" Claude verified: `c:/project/signal_mind/` is a real, separate, live git checkout of the same repo (`origin` = github.com/iRatG/signal-mind, branch `master`), `hf_news.db` confirmed as SQLite format 3 via file signature, reachable via DuckDB `ATTACH ... TYPE SQLITE READ_ONLY`.

**Q (Claude):** Given production needs to run 24/7 and can't depend on a live cross-machine connection — should the harness's data dependency instead be a versioned, frozen, portable headline snapshot (only headline/date/source/language, not full text), rather than live access to `hf_news.db`?

**A (Airat):** "давай как правильно. хорошо?... если мы будем это размещать в сети, чтобы это работало круглосуточно и стабильно, можно из этого выйти?" — confirmed the need for a portable snapshot for production stability. Then, on scope/depth: "только нужные колонки. только нужные данные. только заголовки как мы сказали. может местами глубже. сделать на этом калибровочные коэффициенты. подготовить все для старта. подготовить набор для обучения. индексы у нас есть в глубину год. можно и заголовки забрать в глубину год. и сделать слепок." — confirmed: headline-only, minimal columns, 1-year depth (matching market-side pilot depth), dual-purpose (Ticket 12 calibration + Ticket 06 training set).

**Q (Claude):** Should the sentiment index reuse/adapt the existing `pressure_score_v2`, or be built as a separate, independent metric specifically for the эталон?

**A (Airat):** "нет, надо заново. надо согласно нашим новым знаниям. нашему описанию. нашему диалогу. это важно. отдельно, заново, согласно нашему общению." — confirmed: built fresh, independent of `pressure_score_v2`, grounded in this session's accumulated decisions rather than existing pipeline code.

**Q (Claude):** What method computes the per-headline sentiment score — LLM scoring (DeepSeek) or a lexicon-based method?

**A (Airat):** Redirected up a level before answering the method question directly: "тут математика. тут уже техника. нужно сделать что? нужны метрики, но метрики которые корректируются. не забиты гвоздями. метрики должны быть описаны, для чего, с какой целью. мы их вводим, что мы от них ждем. что они считают. и какие значения для нас хороши после обучения на годе данных, мы все это описываем и даем llm. мы же их будем много проверять. у нас будет ансамбль метрик. мы их будем переиспользовать. но мы должны четко описать что они показывают и какую метрику дают. это основное — чтобы мы могли подать llm и она могла понимать ради чего и зачем и уместна ли она тут." This became the Metric Card registry decision above.

**Q (Claude):** Proposed the Metric Card schema (id/purpose/computation/inputs/expected_range_calibrated/window_applicability/llm_notes) as a Ticket-06-owned, project-wide standard for all metrics — confirm scope, or route to idea-buffer instead?

**A (Airat):** "а ты сам как думаешь? может я не прав с таким подходом. может есть другой. мне интересно твое мнение" — explicitly asked for Claude's independent opinion rather than confirming outright.

**Claude's opinion (given, then revised into the decision above):** endorsed the core idea (comparable to "model card"/"dataset card" practice, consistent with the project's existing registry lineage), but raised three refinements: (1) split "normal/observed range" from "good/bad interpretation" into two separate fields rather than one conflated field; (2) don't force retrofitting Tickets 03/10's existing metrics into the new schema now — apply it to this ticket's new metrics, defer retrofit; (3) LLM's role in the card should be interpretation/appropriateness judgment only, never authority to override Ticket 03's hard blockers.

**A (Airat), responding point by point:** "1 согласен. я может плохо выразился. но все так. есть диапазон. и мы туда делаем. это даже хорошо. может даже сказать медиана. среднюю. минимальное значение. максимум. условно. чтобы мы знали больше о метрике." (confirmed split, added median/mean/min/max as the descriptive fields) "2. я не хочу переписать. но! если в этом есть нужда, то может занесем куда то. или если это концептуально разница, с тем что мы думаем, то давай обсудим. хорошо?" (confirmed: no forced retrofit, but real conceptual conflicts get discussed, not silently ignored) "3. llm больше для описания. у нас все равно должны быть алгоритмы, которые эту метрику будут считать. верно же? они будут описаны они будут формализованы. если мы тут с тобой везде ок и ты дополнишь то фиксируем." — asked Claude to confirm the computation-vs-interpretation split explicitly.

**Claude confirmed:** yes — a formalized, versioned, deterministic algorithm (which may internally invoke an LLM via a fixed prompt template) computes the raw metric value; `llm_notes` is a separate, later-stage interpretation role, never the computation itself. Airat did not object; treated as locked.

**Q (Claude):** For the эталон correlation itself — what lag grid, correlation method, and significance test?

**A (Airat):** "давай скажи как математик у которого в памяти подобные проекты)) друг. можешь помочь мне в этом ответе-вопросе. как лучше? или заложим одно, пропишем, чтобы обратить на это внимание при тестах, сделаем проверочную работу и в итоге опишем гипотезы и почему это выбрали. такой расклад пойдет? но в целом полагаюсь на тебя, математика."

**Claude's recommendation (as given above, in full):** reuse Ticket 10's D-3..D+5 lag grid; Spearman per lag×instrument; Benjamini-Hochberg FDR (not Bonferroni — too conservative for this many correlated tests); block bootstrap for significance (not naive parametric p-values, given likely autocorrelation in the daily sentiment series); lock method before running the calibration pass (pre-registration discipline, prevents p-hacking); one verification run, then document results/reasoning. Also surfaced the structural link: this general correlation becomes the baseline Ticket 10's anomaly-window signals are checked against.

**A (Airat):** "давай фиксируем." — confirmed, no changes requested.

## Working Decision — Track 2 (Qualitative Rubric Review), in progress

### Q4 — Labeled report set: roles, protocol, cadence, interaction shape (resolved)

- **Three-role separation**, extending Ticket 10's "a checker must never check its own work" principle to this ticket's own review loop: (1) **Generator** — the existing report/classification pipeline; (2) **Evaluator** — a separate LLM role/context, never the same call or context as the Generator, that scores the report against the per-signal-type rubric (rubric criteria itself is Q5, still open); (3) **Airat** — periodic spot-audit of the Evaluator's judgments (not primary labeling from scratch). This is the mechanism that actually delivers self-improvement/iteration without making rubric labeling Airat's manual second job: the Evaluator does the scalable first pass, Airat only checks whether the Evaluator itself is still trustworthy.
- **Handoff protocol:** every work item moving Generator → Evaluator → Audit is logged through four explicit states — `taken → in_progress → done → handed_off` — each carrying a timestamp and an explicit from/to reference (see `idea-buffer.md`'s new entry for the general, project-wide version of this pattern).
- **Cadence:** weekly audit sample, not per-run/daily — matches the project's existing preference for infrequent touchpoints.
- **Interaction shape when Airat is involved:** bounded choice only — at most 3 concrete options with reasoning, plus an implicit "your own answer" slot, never an open-ended question (see `idea-buffer.md`'s new entry — this is a standing, project-wide principle now, not unique to this ticket).
- **Known-answer calibration anchors** (CB rate calendar, Rosstat calendar, quarterly earnings/audit reports, MOEX events) — see the Track 1 addendum above; the mechanism was resolved here but belongs conceptually to Track 1's statistical method.

### Q5 — Per-signal-type rubric criteria (resolved)

The rubric is **not** a brand-new, independently invented checklist per type — it's the Evaluator re-verifying each cluster's assigned label against criteria already established across earlier tickets, plus one new mechanism this session filled a real gap for:

- **Main pressure / Rising impulse / Persistent background:** the existing pipeline already distinguishes these by temporal shape (spike vs. rising trend vs. sustained plateau — inherited, not re-decided here). What was missing, and is now resolved: a **domain-relative historical-baseline check** — is this cluster's volume/frequency actually unusual *for its own topic domain* (politics/economy vs. sports/entertainment/culture), rather than judged against one global threshold. Concrete motivating cases from Airat: (a) a genuinely new event that many sources sustain coverage of (the honest case); (b) a manufactured distraction — state media synchronize around a trivial pretext while independent/opposition media cover the real story at lower volume, so raw volume alone would reward the wrong cluster; (c) structural/seasonal noise (e.g. a World Cup) that is *always* high-volume for its domain and must not win "Main pressure" purely on absolute volume. Resolution for (b): cross-check against Ticket 05's existing `category_spread` field — a sharp topic divergence between state-only and independent-only coverage on the same day is itself a signal worth surfacing, not just raw volume. Resolution for (c): compare a cluster's volume against the **calibrated normal distribution for clusters of its own domain** (from the 1-year snapshot), not a single global number — "unusual for politics" and "unusual for sports" are different bars.
- **Computation mechanism for the domain-relative baseline ("has this happened before, for this domain"):** reuses the existing **Ouroboros SQL-hypothesis pattern** already implemented in this repo (`src/agent/agent.py`, `src/agent/experiments.py` — LLM writes a SQL query, retries on failure across `sql_v1/v2/v3`, executes, logs `confirmed`/`signal_score`/`finding`) rather than building a parallel bespoke query-writer. Same mechanism, different subject matter (domain-relative historical cluster frequency instead of news↔market correlation hypotheses). Confirmed technically portable to a future server deployment — plain Python process, file-based storage (SQLite/DuckDB), external LLM API calls already network-based, same deployment pattern the existing systemd-timed news-pressure regimen already uses; local hardcoded paths/env vars are Ticket 09's territory, not a new problem introduced here.
- **Synchronized story:** existing `category_spread` (Ticket 05) is the check — genuine cross-spectrum agreement vs. only state-aligned unanimity.
- **One-source anomaly:** existing Ticket 03 flags (`single_source_persistent`, `single_source_high_volume`).
- **Noise/routine:** residual category — nothing above triggered.
- **When the Evaluator disagrees with the Generator's original label:** logged as an evaluation signal only (verdict + reasoning) — does **not** retroactively alter the already-sent report. Track 2 is a measurement/improvement loop, not a second live gate stacked on top of the already-closed Ticket 03 quality-gate design. A single disagreement is just data; a *persistent, previously-unseen* disagreement pattern is the trigger for the next mechanism.

### New project-wide mechanism: Decision Block (calibration role)

A fourth role, alongside Generator/Evaluator/Airat-audit, specified with the same rigor as the others:

- **Trigger:** a mismatch between the system's expectation and the Evaluator's judgment that persists across an observation window (illustrative figure: 5 iterations) *and* has not occurred in this specific shape before in history — not any single disagreement, which is just noise.
- **Required justification before acting:** the Decision Block must reconcile three things and document the reconciliation: (a) the metric's documented `purpose` (from its Metric Card), (b) the documented decision algorithm that produced the original label, (c) the specific way the Evaluator's expert judgment diverged. Calibration is only justified once this reconciliation is written out — never a bare "confidence dropped, adjusting" with no explanation.
- **What gets calibrated:** either the **label** (the classification logic/threshold) or the **metric itself** (its computation/interpretation_bands) — the Decision Block must be explicit about which, since they're different fixes for different root causes.
- **Self-documentation is the safety mechanism:** every calibration decision files its own ticket and database record, written by the system itself — the justification, not just the new number, is what's preserved. This is what makes the decision auditable rather than an opaque internal weight change.
- **Human touchpoint:** the Decision Block's self-documented proposal is exactly what surfaces in Airat's existing weekly bounded-choice audit (accept the proposed recalibration / reject and extend the observation window / Airat's own alternative) — the Decision Block does the observation, reasoning, and documentation; Airat's part stays a fast confirmation, consistent with the already-established interaction principle, rather than requiring him to re-derive the analysis himself.
- **Closing the loop:** after a calibration is applied, a fresh observation window (e.g. another 5 days) checks whether the specific conflict actually stopped recurring — confirming the system's decision-making genuinely improved, not just moved the discrepancy elsewhere.

Airat's own framing, verbatim, captures the core intent behind this mechanism (see transcript below) — flagged by him as one of the more important findings of this whole session.

**Loop-prevention safeguard (resolved, applies to the Decision Block generally, surfaced while resolving Q6 below):** before proposing a new calibration, the Decision Block must check its own calibration history via the Metric Card's version log. If the trigger condition is genuinely novel (no prior calibration attempt addressed this specific issue), proceed as designed above. If the *same* underlying issue recurs after it was already calibrated once, that is no longer treated as "a fresh unprecedented pattern" — it's evidence the automated fix isn't working. **Cap: after 3 failed calibration attempts on the same recurring issue, stop attempting further silent recalibration and escalate to Airat as an explicitly flagged unresolved case** ("this isn't self-resolving, needs your judgment"), rather than repeating a fourth attempt automatically. Prevents the self-calibration loop from oscillating or repeating indefinitely without ever surfacing that it isn't converging.

**Refinement (same session, Airat corrected this before moving to Q7):** the 3 attempts must be **3 genuinely different, independent approaches to the same underlying issue — never the same fix retried 3 times.** Each attempt's record must explicitly carry forward what the previous attempt tried and why it failed, so the next attempt is constrained to avoid repeating a known-failed approach — not just three independent shots in the dark. Airat's framing: "только 3 разных подхода, но изучить один и тот же смысл. а то 3 одинаковых попытки — тоже тупость. разные три независимых проверки. и в проверки запоминать предыдущую проверку: мол, я так сделал, результат такой, нас не устроил, больше так не делай, делай по-другому."

### Q6 — Quality-gate/delivery-block metrics (resolved)

Reuses the exact Q5 mechanism (Decision Block + the Ouroboros SQL-hypothesis agent) rather than inventing a separate one — applied to a different trigger: persistent quality-gate blocking (Ticket 03's hard/soft blocks firing repeatedly) instead of a classification mismatch.

- **Observation window, two thresholds:** 7 days as a soft watch period for a suspicious, persistently-flagged news pattern/value; **30 days as a hard ceiling** — if the issue is still occurring after 30 days, that's treated as a confirmed calibration gap for that specific news pattern or value, not noise.
- **Diagnostic mechanism:** identical to Q5 — a one-time invocation of the Ouroboros-style SQL-hypothesis agent, run off-cycle (e.g. overnight), checking whether this exact pattern occurred before in the available historical depth (illustrative example: 6 months).
- **Self-documentation format:** the same ticket-shaped trace as Q5 — observed anomaly → suspected correlation gap → which agent was invoked → what query it composed → what historical depth was checked → found/not-found → resulting decision (close the ticket, or take a specific corrective action).
- **Data-sufficiency side effect:** if the diagnostic step concludes the available historical depth/breadth is too shallow to judge honestly, that is itself a valid outcome — routed to Ticket 12 (calibration study) / Ticket 05 (source strategy, deeper backfill) rather than treated as a Ticket 06 problem to solve directly.

### Q7 — Cluster stability regression checks (resolved)

Resolved as a special case of a broader **champion/challenger deployment pattern**, which Airat brought directly from real production ML experience:

- **Champion** = the current live news-pressure pipeline, stable, actually producing delivered reports.
- **Challenger** = any candidate change (code, threshold, prompt) — runs in shadow, on the same real or historical data, in parallel, writing to a **separate table/store**, never touching what's actually delivered.
- **Target metrics** = exactly this ticket's already-built metric set (эталон correlation, per-signal-type rubric scores, quality-gate stats, and — the new one this question adds — a cluster-stability diff against a fixed historical sample, e.g. "does the CB-rate cluster still group the same ~12 articles together, or does it fragment/merge unexpectedly").
- **Promotion rule:** a challenger is only promoted to become the new champion if it does not regress the target metrics relative to history/the current champion. Airat's framing: "нельзя ставить на пром [что-то], которое сильно выбивается по таргет-показателям — это всю систему поломает."

**Ownership split (Claude's recommendation, accepted):** Ticket 06 owns *what "good enough" means* — the target-metric definitions/Metric Cards themselves (already built in this ticket). Ticket 11 (Agentic Delivery Process) owns *the shadow-run mechanics* — how a challenger actually executes in parallel, where its output is stored, how long it observes before a promotion decision is considered. This is the same "stable interface, free-changing internals" split already established between modules elsewhere in this project.

**Unification (Claude's recommendation, accepted) — one Decision Block, not three separate governance mechanisms:** rather than inventing a distinct decision-making pathway for promotion, this reuses the *same* Decision Block already specified in Q5/Q6 — observe, check its own history, self-document, propose, and (if within the 3-attempt cap) surface via the same weekly bounded-choice checkpoint with Airat. The Decision Block now has three known trigger types feeding the same mechanism: (1) a persistent, previously-unseen classification mismatch (Q5), (2) a persistent quality-gate block (Q6), (3) a challenger's target-metric evaluation ready for a promotion decision (Q7). One well-specified decision engine, three inputs — not three parallel, independently-invented ones.

### Q8 — Review-notes format (resolved)

The Decision Block's self-documented tickets (Q5/Q6/Q7) only capture *exceptional* moments — when something triggered a calibration or promotion decision. Without a routine record of the ordinary weeks, there's no way to answer this ticket's own opening question ("is the radar getting better over time") — only a log of failures, never a trend line.

- **A lightweight, routine "system health record" is written every week**, alongside the existing weekly bounded-choice audit — regardless of whether anything triggered escalation: date, number of reports/clusters checked, Evaluator/Airat agreement rate, and current values of the key target metrics. This is intentionally cheap to produce (no new analysis, just a snapshot of numbers already being computed) — the point is continuity of history, not a new workload.
- **Stability itself is not automatically "good news."** Airat's point: a long stretch of calm agreement could mean genuine stabilization, or it could mean data stagnation (e.g. an unusually quiet news period, or the Generator/Evaluator both drifting into a degenerate agreement) — the health record's job is only to preserve the raw trend; *interpreting* an unusually long calm streak is itself a candidate future trigger for the same Decision Block/Ouroboros-style historical check ("has agreement been this flat for this long before?") — not decided as an active mechanism now, just noted as a natural extension once the health-record history actually exists.

### Q9 — Scenario/stress-test replay through different threshold configs (resolved, no new mechanism)

Closes without inventing anything new: this is the Q7 champion/challenger pattern applied to the frozen 1-year snapshot (Track 1) as the arena instead of live data — several challenger configurations (different threshold/parameter-registry versions) run side by side against the same fixed historical snapshot, compared against the same target metrics. Same mechanism, historical input instead of live input. Confirmed by Airat by deferring to accumulated session judgment rather than re-litigating: "давай со всеми ограничениями, какие есть у нас, и как мы дискутировали, примем профессиональное мышление на этот счёт."

All nine original questions for Ticket 06 (Track 1 + Track 2) are now resolved. See the top-level status/claim header for closure.

## Grilling Transcript — Track 2

**Q (Claude):** Who labels the reports, at what cadence, and how is the sample constructed?

**A (Airat):** Redirected to the underlying motivation before answering directly: "мы поэтому и привлекаем llm. мы поэтому так долго все обсуждаем, все расписываем. потому что мы хотим по циклу запускать и улучшать процесс. мы хотим самообучения. мы хотим итераций. и чтобы система проверяла сама себя. верно же?"

**Claude:** confirmed the self-improvement goal, but flagged that "the system checks itself" must not mean the same LLM call/context grades its own output (ties directly to Ticket 10's already-decided "checker ≠ the thing being checked" principle) — proposed the three-role Generator/Evaluator/Audit split above.

**A (Airat):** "да. все разделяем. только договариваемся об общении между собой. договариваемся об интерфейсе между модулями. я взял в работу от того-то (или это я взял). я работаю. я закончил. я отдал. ок хорошо. включен второй модуль. я взял работу от того-то (или это я взял). я работаю. я закончил. я отдал это. и каждый блок делает свою работу. не прикрывает другого." — this became the four-state handoff protocol above.

**A (Airat), continuing unprompted on the audit-interaction shape:** "меня не часто наверное привлекать. я все равно буду разбирать с твоей помощью... лучше если ты мне поможешь делать выбора. допустим 3 варианта. ну или еще 4 если я что-то своё скажу... а так 3 варианта для меня я выбрал и идём дальше. чтобы действительно это не стало моей второй работой. ну и чтобы система не думала что я узкое горлышко. потому что ты умнее в этом проекте будешь чем я, поэтому мы все подробно описываем. все зафиксируй тут." — became the bounded-choice interaction principle above.

**Q (Claude):** For the per-signal-type rubric itself (Q5) — asked Airat, at his own request, to slow down and answer a simple, concretely-framed question: "when you personally read a report and see a signal-type label, what convinces you it's right, and what makes you feel it's wrong?"

**A (Airat):** Answered with a concrete worked example instead of an abstract rubric: the CB key-rate decision → RUB/USD volatility relationship (real, "100%" known from experience — volatility rises in the week before a rate decision, with a specific intraday pattern around 15:00). Proposed using this and similar known relationships as calibration anchors that the whole coefficient/threshold system gets scaled against, so that new/unseen patterns get classified by comparing their coefficients to these known reference points. Asked explicitly for correction: "разве я не прав? но поправь меня."

**Claude's response:** agreed with the core idea (a recognized, sound practice — "known-answer test" / positive-control validation), with one refinement: don't extrapolate one event type's exact coefficients wholesale to unrelated topics; use known anchors to validate that the general statistical method works, not as the sole source of every threshold. Asked whether Airat had more such known anchors besides the CB-rate example.

**A (Airat):** generalized to a class of known calendar events (CB calendar, Rosstat calendar, quarterly earnings/audit reports, MOEX events) rather than naming a second isolated example, then introduced two adjacent ideas mid-thought: (1) MOEX itself as a **news** source, stated as a firm addition ("но точно нужно добавить... 100% давайте добавим"); (2) a RAG layer over public quarterly audit reports/financial disclosures, which he explicitly deferred himself ("это уже что-то другое, это рак [RAG] система, наверное в других версиях... можно как в туду записать").

**Claude:** proposed routing: the known-calendar-anchor mechanism stays in Ticket 06 (recorded in the Track 1 addendum above); the MOEX-as-news-source proposal is recorded as a forward note on the already-closed Ticket 05, not re-decided here; the RAG idea goes to `idea-buffer.md` as a deferred future-version item.

**A (Airat):** "да, давай фиксируем. мы общаемся, думаем, идеи приходят, пишем в бэклог. главное фиксировать, чтобы мы друг друга понимали." — confirmed the routing. Recorded to `05-source-strategy.md`, `idea-buffer.md` (two additional entries), and this ticket's Track 1 addendum in this session.

**Q (Claude):** Simplified the rubric question to one concrete type — "Main pressure" — and asked plainly: what would personally convince Airat a cluster labeled 'Main pressure' really is the most important thing that day, versus just the loudest?

**A (Airat):** gave three distinct scenarios rather than one criterion: "ну какое-то событие которого не было, все об этом начали писать, все будут мусолить... или наоборот, что-то произошло, государственным СМИ не выгодно говорить о плохом, нашли какой-то другой дурацкий повод, его все гос СМИ обсуждают, а вот оппозиционные и другие будут обсуждать другую истинную новость. ну и ещё один из вариантов, что шум специально созданный. ну или сезонность. чемпионат мира. все обсуждают его. на его фоне много может решений принимают политических, но их не видно, потому что новостной шум другой... тут надо разбираться, конечно."

**Claude:** organized these into (1) genuine broad event — honest case; (2) manufactured distraction — state-only high volume masking a true, lower-volume independent-media story; (3) seasonal/structural noise (World Cup) crowding out real signal. Proposed handling (2) via Ticket 05's existing `category_spread` field (topic divergence between state-only/independent-only coverage as a signal) — confirmed by Airat ("да согласен") — then proposed handling (3) via comparing volume against a domain-relative historical baseline rather than one global threshold, using the World Cup vs. a modest-but-unusually-covered political decision as the illustrative contrast.

**A (Airat):** "именно!! молодец! верно. тем самым мы будем отличать экспертно новости в определённой области между собой. потому что у них будет своя частотность. и какой-то показатель в истории. 'мол а было ли такое, что....' и есть какой-то проверяльщик гипотез условно, какой-то простой составитель sql запроса и вывода метрики, надо наверное тут подключать навык llm писать запрос к базе данных. как считаешь?"

**Claude:** identified that this already exists in the repo as the Ouroboros SQL-hypothesis engine (`src/agent/agent.py`/`experiments.py`, confirmed via the earlier background research pass) and recommended reusing that mechanism rather than building a parallel one, extending it to Rising impulse and Persistent background too (both also fundamentally "is this unusual relative to its own history" questions).

**A (Airat):** agreed, then asked a deployability question: "пока будем гонять всё на компе, стабилизируемся, будем потихоньку выводить в пром на сервер... я к тому, что ouroboros мы сможем переиспользовать, верно же?"

**Claude:** confirmed technical portability (plain Python process, file-based SQLite/DuckDB storage, already-networked LLM calls, an existing systemd-timer deployment precedent elsewhere in the project) — local path/env-var hardcoding is Ticket 09's concern, not a new blocker.

**Q (Claude):** proposed closing Q5 by treating the rubric as a re-verification against already-established criteria (Tickets 02/03/05) plus the new domain-baseline mechanism, rather than a wholly new rubric — and separately asked what happens when the Evaluator disagrees with the Generator's original label (logged as a signal only, vs. an active live override of the sent report).

**A (Airat):** confirmed logging-only for a single disagreement, then extended it: "если это из раза в раз, значит на какой-то итерации мы будем менять коэффициент, корректировать, автокалибровку делать."

**Claude:** asked whether this auto-calibration proposes a new value and waits for Airat's confirmation (via the existing bounded-choice pattern) before applying it live, or applies fully autonomously with only after-the-fact notification.

**A (Airat):** reframed the question itself as too narrow, and described the missing piece directly: a dedicated **decision-making block**, specified as rigorously as every other module, that watches for a mismatch which "происходит после 5 итераций... что ранее в истории не было", and before calibrating must "попробовать объяснить как это поменять, для чего эта метрика была, как мы принимали решения — у нас будет документация на эту тему... если эта метрика, которая описана, чётко ради чего она сделана, не бьётся с тем экспертным заключением этого агента — значит, мы сверяем описание метрики, описание алгоритма принятия решений, и калибруем — либо метку, либо метрику — и это будет обосновано... заведём тикет, занесём в базу данных, то есть решение будет описано самой системой... и после этого живём снова 5 дней — если калибровка сработала, конфликтов больше не будет возникать." Asked explicitly for confirmation: "верно же понимаю, мы с тобой в этом сходимся, правильно?"

**Claude:** reflected the full chain back (Decision Block as a fourth role; trigger = persistent + previously-unseen mismatch; mandatory justification reconciling metric purpose + decision algorithm + Evaluator's divergence before acting; calibrates label or metric explicitly, not implicitly; self-documents via its own ticket + DB record; proposal surfaces through the existing weekly bounded-choice audit rather than a separate approval flow; a fresh observation window afterward confirms the fix actually worked) and asked whether this matched Airat's intent or whether he meant fully autonomous application without human confirmation.

**A (Airat):** "ну идеально. раз мы с тобой тут сходимся. по-моему это очень важная находка. как я считаю. это очень важно. согласен." — confirmed the full framing, including that the Decision Block's proposal flows through the existing weekly bounded-choice checkpoint rather than applying fully autonomously, and flagged the Decision Block mechanism itself as one of the session's more important findings.

**Q (Claude):** For Q6 (quality-gate/delivery-block metrics) — asked, with a concrete example (a report blocked 5 of 7 days), how to tell a genuinely bad data week apart from an over-nervous, miscalibrated gate.

**A (Airat):** answered by directly reapplying the Q5 mechanism to this new trigger: "наверно да, это пример того, что мы как-то не совсем красиво обучились... надо какой-то фрейм дней поставить, чтобы следить за такой новостью, условно там 7 дней, максимум 30 дней — если на протяжении 30 дней такое плохое происходит, то это точно какой-то провал в калибровке." He also flagged that calibration quality depends on training-data breadth/depth and floated writing a broader collection parser if needed, then restated the diagnostic mechanism from Q5 explicitly for this case: "нужен какой-то агент единовременный, составитель SQL-запроса, который просто подобного рода паттерн проверяет на исторических данных... запускаем ночью и делаем проверку по конкретному тикету", followed by the same self-documenting ticket trace (saw anomaly → suspected gap → invoked agent → composed query → checked N months of history → found/not found → close ticket or act). "По-моему всё логично, и главное, чтобы была такая возможность такого рода проверку сделать."

**Claude:** confirmed this reuses Q5's mechanism rather than inventing a new one, proposed the concrete 7-day/30-day thresholds, and routed the data-sufficiency tangent to Ticket 12/05 rather than deciding it here.

**A (Airat):** confirmed ("да"), then raised a new safety concern unprompted: "нам важна самопроверка. система обучается, проверяет, думает, фиксирует, по заданным правилам. главное продумать, чтобы не попадала в цикл. главное, чтобы не было бесконечности одного и того же."

**Claude:** proposed the loop-prevention safeguard above — the Decision Block checks its own calibration history (via the Metric Card's version log) before proposing a new fix; a recurring issue that already went through one calibration attempt is no longer "novel" and must not trigger silent repeated attempts — and asked how many failed attempts to allow before mandatory escalation to Airat.

**A (Airat):** "3 давай поставим. и ок." — capped at 3 failed attempts before escalation.

**Q (Claude):** For Q7 (cluster stability) — asked, using a concrete example (a code change causing a previously-coherent 12-article CB-rate cluster to fragment or merge unexpectedly), how to catch this before a broken report reaches Airat.

**A (Airat):** answered from real production experience rather than the news-pressure domain directly: "на работе мы делаем так: есть пром решение, оно стабильное, работает — это контур, называется прод. и есть другая команда, которая крутится вокруг этих прод-данных, проверяет различные гипотезы, собирает факторы, занимается скорингом, и всё время смотрит на то, как это было в проме... есть какие-то таргет-метрики, таргет-показатели, на которые они смотрят, и этот новый подход должен калиброваться с тем, что было на проме — нельзя ставить на пром то, что не вписывается в целевые метрики, это всю систему поломает... есть какой-то план, который работает, основываясь на метриках, которые мы выявили, и есть другой процесс вокруг — два разных уровня, которые всё время калибруют, перепроверяют, возможно в другую таблицу записывают, но всё время сверяют свои показатели: попали, не попали."

**Claude:** mapped this onto a standard champion/challenger shadow-deployment pattern, proposed the ownership split (Ticket 06 = target-metric definitions, Ticket 11 = shadow-run mechanics), and asked Airat to confirm the split.

**A (Airat):** pushed back on being asked to just confirm, and asked for Claude's own opinion directly: "а ты сам как бы поступил? у тебя у самого какое мнение? давай сделаем симбиоз на эту тему?"

**Claude's opinion (given, then accepted):** the ownership split stands, but more importantly — don't invent a third, separate promotion-decision mechanism. Reuse the exact same Decision Block already specified for Q5 (metric-mismatch calibration) and Q6 (quality-gate diagnosis): observe, check its own history, self-document, propose, escalate via the same weekly bounded-choice checkpoint within the same 3-attempt cap. One decision engine with three trigger types, not three independently-invented governance mechanisms.

**A (Airat):** "давай так и сделаем." Also gave standing collaboration feedback: "ты меня тоже корректируй, в рамках того, что уже описали. ты профессионал, ты документируешь, ты знаешь, ты уже видишь в целом систему, у тебя уже есть ограничения [awareness of what's consistent with prior decisions]. но и у меня как у человека есть опыт. мы из этого строим фреймворк." — explicitly asking Claude to proactively flag inconsistencies against prior decisions, not just implement whatever is proposed at face value.

**Q (Claude):** For Q8 (review-notes format) — noted that the Decision Block's tickets only capture exceptional/triggered moments, and asked whether a lightweight routine weekly "health record" should also be written even when nothing triggers escalation, purely so a long-term trend line can exist later.

**A (Airat):** "лучше фиксировать. да. записывать. система в целом увидит, что видимо какой-то штиль в данных, какая-то стабилизация, стагнация — может тоже это какое-то имеет значение. но фиксируем, будет ясно, анализируя такие случаи." — confirmed, and added that a long calm/stable streak is not automatically good news — it could indicate genuine stabilization or data stagnation, and this ambiguity itself is worth being able to analyze later using the recorded history.

**Q (Claude):** For Q9 (scenario/stress-test replay through different threshold configs) — proactively flagged (per Airat's own request to be corrected/challenged, not just agreed with) that this looked like it needed no new mechanism at all: running the Q7 champion/challenger pattern against the frozen 1-year snapshot instead of live data, with several config versions as parallel challengers, already covers it. Asked Airat to confirm rather than deciding unilaterally.

**A (Airat):** "давай со всеми ограничениями, какие есть у нас, и как мы дискутировали, примем профессиональное мышление на этот счёт." — deferred to the accumulated session judgment, confirming the no-new-mechanism closure.

All nine questions for this ticket (Track 1: data source, sentiment index, Metric Card schema, statistical method, known-answer anchors; Track 2: roles/protocol/cadence/interaction, rubric + Ouroboros reuse + Decision Block, quality-gate diagnosis + loop-prevention, cluster stability via champion/challenger, review-notes format) are now resolved.
