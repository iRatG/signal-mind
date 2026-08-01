# Ticket 03 - Quality Gate Contract

Status: closed
Type: grilling
Labels: `wayfinder:grilling`
Claim: unclaimed
Blocks: Ticket 06 - Evaluation Harness, Ticket 07 - Delivery Policy, Ticket 08 - OpenSpec Bridge

## Question

What quality gate should decide whether a report is allowed to be sent?

## Why This Matters

The regimen already writes `send_allowed`, `block_reason`, and quality flag counts. On 2026-07-28 the daily run had 1 flag and was blocked. The product decision is whether that behavior is correct or too strict.

## Decision Shape

Set policy for:

- collector errors;
- empty article or cluster counts;
- unsupported numeric claims;
- one-source persistent clusters;
- thin clusters in daily versus weekly/monthly/history modes;
- LLM fallback to heuristic labels;
- maximum tolerated quality flags by mode;
- whether a blocked report should be silently stored, sent with warning, or escalated to Airat.

## Starting Assumption

Daily reports should tolerate a small number of low-severity flags only if the flagged cluster is excluded from the main signal. Weekly/monthly/history reports can remain dry-review artifacts until evaluation is stronger.

## Forward Note (2026-07-31, captured mid-grill, spun out into Ticket 12, not resolved)

The daily=30%/weekly=50% cluster-exclusion split and the per-flag severity table (`unsupported_number`=low, `single_source_persistent`=medium, `single_source_high_volume`=medium, `thin_cluster`=low, `llm_fallback_heuristic`=medium) discussed above are explicitly provisional — round numbers picked to keep this conversation moving, not measured values. Airat asked not to spend further grilling time on the exact gradations before [Ticket 12 - Historical Cluster Calibration Study](12-historical-cluster-calibration.md) produces real cluster-size/persistence/source-spread distributions from the historical archive; that ticket runs separately and does not block the rest of this grilling session. When Ticket 12 closes, revisit only these two specific numbers via a short revision pass — not a full re-grill of this ticket — unless the empirical gap turns out to be large.

## Working Decision

**Hard blockers (unconditional, any mode) stay exactly as implemented today**: `run_failed`, `collector_errors` (collector error count > 0), `no_articles` (0 articles), `no_clusters` (0 clusters). These block the whole report — there is no "send with warning" variant for a missing/broken input, only for a quality concern about data that does exist.

**New cross-cutting rule: no silent blocking, ever.** Every block — hard or soft — must escalate to Airat with the specific reason, never just sit unsent with no notice. This applies uniformly regardless of which rule below triggered it. (A follow-on idea from this same discussion — a diagnostic agent that reads logs and explains *why* a run failed without auto-patching code — is out of scope here and recorded as a forward note on [Ticket 11](11-agentic-delivery-process.md).)

**Architecture shift: per-cluster severity replaces all-or-nothing report blocking.** The current code blocks the *entire* report once `quality_flag_count` (summed across all clusters) exceeds a single global threshold (default 0). That is replaced by a two-tier severity model per cluster:

- **Severity (a) — annotate, keep in main signal.** The cluster stays in the report's main signal; the flag is surfaced as an inline note on the affected claim/cluster. Applies to:
  - `unsupported_number` — a numeric claim in the LLM interpretation isn't backed by the cluster's own evidence text.
  - `thin_cluster` (non-daily modes only, per existing code) — small volume/source-spread is expected and tolerable in weekly/monthly/history aggregates.
- **Severity (b) — exclude from main signal, move to a "needs review" section, report still sent.** Applies to:
  - `single_source_persistent` — one source, persistence ≥ 14 days.
  - `single_source_high_volume` — one source, volume ≥ 75. (This flag exists in code but wasn't in the original ticket text — formally adopted here as a real, named flag type with the same severity as its sibling.)
  - `llm_fallback_heuristic` — **new flag, not yet implemented.** Fires when a cluster's interpretation came from the heuristic fallback instead of a successful LLM (DeepSeek) call. Currently this fallback happens silently with no flag at all — that gap is closed here. Severity (b) because an unconfirmed/rougher interpretation shouldn't be presented as part of the trusted main signal, even though the underlying cluster (the set of articles) is real.
- No per-cluster flag on its own escalates to a full-report block — only the hard blockers above do that directly. A report-level cap on the *share* of severity-(b) exclusions (originally proposed as daily 30% / weekly-monthly-history 50%) was discussed as the mechanism for "too many exclusions means escalate the whole report as blocked instead of sending a hollowed-out one" — but Airat asked not to fix that number by guesswork. It is explicitly **provisional**, spun out to [Ticket 12 - Historical Cluster Calibration Study](12-historical-cluster-calibration.md), and does not block this ticket's closure (see the Forward Note above).

**Consistency with existing project principles:** every threshold here (the 14-day/75-volume/30%/50% numbers) lives in the versioned parameter registry established in [Ticket 02](02-signal-ontology.md), not hardcoded — this ticket sets the *policy shape and severity*, not final calibrated numbers. Airat's framing for this session: keep the system "dynamic, re-tunable, not hardcoded — a living organism," which this registry-based approach is meant to satisfy directly.

**Scope note carried over from this session:** the code referenced throughout this ticket (`analytics/news_pressure_cluster.py`, `scripts/news_pressure_regimen.py`) is prototype/spike code used for concrete grounding, not a locked-in architecture — the real implementation at Ticket 08/implementation time is expected to diverge from these exact names/structures once this contract is settled.

## Grilling Transcript

**Q1.** Should the hard blockers (`run_failed`, `collector_errors`, `no_articles`, `no_clusters`) remain an unconditional block of the whole report in any mode, with no "send with warning" option?
Recommendation offered: yes, keep as unconditional — there's no signal to salvage when the input itself is missing or broken.
Airat's answer: agreed in substance, but added a requirement I hadn't asked about — any failure (agent didn't run, data missing, something crashed) must be *communicated*, never silent. He also floated a "support agent" that walks logs, finds which step broke and why, and proposes a fix or restart — explicitly **not** one that auto-rewrites code or patches things itself, because the cause might be a transient blip (network, server reboot) rather than a real bug; that needs its own discussion later. We agreed to record that idea as a forward note on Ticket 11 (Agentic Delivery Process) rather than resolve it inside this ticket, and Airat confirmed: "да. согласен... из этого мы обсуждаем все что может зайти в систему. и ты фиксируешь все это."

**Q2.** Should quality-flag blocking move from all-or-nothing (whole report blocked once total flags exceed a threshold) to per-cluster: exclude the flagged cluster from the main signal, send the rest?
Recommendation offered: yes — one anomalous cluster shouldn't kill an otherwise-fine report, and the 2026-07-28 example (1 flag → whole daily report blocked) already shows the cost of all-or-nothing in practice.
Airat's answer: "да. делаем логично. четко... система будет динамичная, перестраиваемая, не хардкод... живой организм. принимаю твою рекомендацию."

**Q3.** With per-cluster handling confirmed, what severity (annotate-only vs. exclude-from-main-signal) applies to each flag type? Proposed table: `unsupported_number`=low/annotate, `single_source_persistent`=medium/exclude, `single_source_high_volume`=medium/exclude, `thin_cluster`=low/annotate, plus a new `llm_fallback_heuristic`=medium/exclude to close a gap where LLM-fallback currently isn't flagged at all.
Airat's answer: accepted the approach, then added an important meta-clarification rather than adjusting the table itself: the referenced code is a prototype for grounding the conversation, not the final implementation — "мы с тобой общаемся для того, чтобы создать фреймворк... реализовывать будем более четче, более качественно." He also raised a cost/role idea for later — a cheap, non-conversational coding-only model for pure implementation steps, saving the pricier conversational model for interpretation/dialogue — recorded as a forward note on Ticket 11, not resolved here.

**Q4.** What report-level threshold (share of severity-(b) exclusions) should trigger escalating the *whole* report as blocked rather than sending it with exclusions? Proposed starting values: daily 30%, weekly/monthly/history 50%, framed as a percentage (not absolute count) living in the versioned parameter registry.
Airat's answer: rejected picking a number by guesswork. He proposed instead a separate empirical study over the historical news archive (which he described as "6 years, American and Russian databases") — a one-off analytics module that runs once, trains over 2-3+ years of data, and derives explicit cluster boundaries, so we don't have to argue over exact gradations inside this conversation. This became [Ticket 12 - Historical Cluster Calibration Study](12-historical-cluster-calibration.md). Research during that spinoff corrected the dataset description: `db/hf_news.db` is not two matched 6-year national corpora — it's ~4.5 years of English financial news (2021-01 to 2025-09) plus a thin Russian tail (a few months only). That correction is recorded on Ticket 12 and on [Ticket 05](05-source-strategy.md).

**Q5.** Should `llm_fallback_heuristic` be a real flag, and at what severity?
Recommendation offered: yes, and severity (b)/exclude — a heuristic label is materially less trustworthy than an LLM interpretation, and a cluster with unconfirmed interpretation shouldn't sit in the "main signal" bucket unflagged.

## Revision (2026-08-01, from Ticket 12 - Historical Cluster Calibration Study)

The Q4 exclusion-share thresholds (30%/50%) were **not evaluated** by Ticket 12 — that requires running `quality_flags()`'s actual logic (unsupported-number checks, single-source-persistent, etc.) against the historical archive, which was out of scope for what Ticket 12 actually ran (cluster-size/persistence/source_spread distributions only, not quality-flag simulation). Also worth noting: the "6 years, American and Russian databases" framing referenced above in Q4 is now further corrected — the real composition is 14 distinct sub-datasets (56% `fnspid_news`), not a single named corpus; see [Ticket 12's findings report](../calibration/findings-report.md) for the full breakdown. The 30%/50% question remains genuinely open — a future session should either run the quality-flag simulation or make a reasoned starting-value decision without it, but not silently inherit an unvalidated guess.
Airat's answer: "да. согласен."
