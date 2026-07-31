# Ticket 07 - Delivery Policy

Status: closed
Type: grilling
Labels: `wayfinder:grilling`
Claim: claimed by Claude (wayfinder session, 2026-07-31)
Blocked By: Ticket 01 - Product Contract, Ticket 03 - Quality Gate Contract, Ticket 04 - Report Prototype (all three closed 2026-07-31 — this ticket is now unblocked)
Blocks: Ticket 08 - OpenSpec Bridge

## Question

When, where, and how should News Pressure Radar send reports automatically?

## Why This Matters

The timer is running, but delivery is intentionally not enabled. Delivery changes the risk profile: a bad report becomes an interruption, and a public report becomes reputational output.

## Decision Shape

Decide:

- channel: Telegram DM, MetaStore, email, Obsidian note, or none;
- timing: daily morning, weekly review, or manual pull;
- failure behavior: silence, warning, or status ping;
- content length by channel;
- whether blocked reports should be summarized;
- who can receive reports later.

## Starting Assumption

Start with Telegram DM to Airat only, daily, only when `send_allowed=true`; send blocked-status only after repeated failures or explicit request.

## Forward Note (2026-07-31, captured while grilling Ticket 01, not resolved)

Airat mentioned either email or Telegram as acceptable daily channels, without ranking one over the other — confirms the Starting Assumption's direction but doesn't lock the specific channel yet. Decide for real when this ticket is claimed.

**Resolved 2026-07-31 (this ticket's own grilling session):** not ranked — both channels are active at v0, at once, with genuinely different formats. See `## Working Decision` below.

## Forward Note (2026-07-31, captured while grilling this ticket, not resolved)

Airat argued Telegram's "operational" nature (he and any future reader check it more often than email) makes it well suited to real-time alerts — e.g. a Telegram-only push the moment a cluster spikes sharply mid-day, outside the normal daily schedule. Explicitly deferred: v0 stays on the single daily schedule decided below, no intraday/real-time alert path. Revisit only as a v1+ feature, and only once the daily cadence has actually run for a while — building an intraday alert now would need a new collector/detection cadence that doesn't exist yet (`scripts/news_pressure_regimen.py` only supports `daily/weekly/monthly/history`, no hourly mode), plus a spam-guard design, neither of which is justified before the daily loop itself is proven.

## Forward Note (2026-07-31, captured while grilling Ticket 04 - Report Prototype and Ticket 10 - Market Coupling Model, not resolved)

Airat sketched a distinct **Editor role** for chart/color/visual polish (a "beautiful chart" for the report, tone/seriousness/date formatting — "the face of the project"), deferred by both those tickets to this one.

**Resolved 2026-07-31 (this ticket's own grilling session), partially:** v0 stays text-only in every channel — no charts, no color/visual styling — even though a working chart-generation pattern already exists elsewhere in the repo (`analytics/gen_report_charts.py`, matplotlib/Agg, built for the unrelated marathon/habr report — its output under `analytics/marathon_charts/` is protected by `CLAUDE.md` and must not be touched). Designing an actual chart for the daily digest (which metric, what style, how it embeds per channel) is real, undecided work, deliberately left as an open implementation item rather than solved inside this delivery-policy ticket. Tone/color/seriousness guidelines are likewise still open. Resolve when report rendering is actually implemented.

**Reference material added 2026-07-31, after this ticket closed:** Airat dropped three financial-report/dashboard screenshots into a local `dashboard/` folder as loose visual inspiration for this eventual chart/Editor work (an infographic-style single-page company analysis, a detailed multi-panel LSEG stock report, and a live-dashboard-style stock page) — explicitly "just a general suggestion... maybe a layout/mockup," not a decision, and explicitly not for git (`dashboard/` added to `.gitignore`, stays local-only). Common thread across all three worth noting for whoever does this work: compact KPI/number tiles up top, one dominant chart, then supporting breakdown panels below — useful as a mood-board, not a spec.

## Working Decision

Resolved 2026-07-31 (`/grilling` session, this ticket's largest single-session grill so far — 9 primary questions plus a 3-level decision-tree pass on failure/noise-day behavior). Full raw Q&A is in `## Grilling Transcript` below; this section is the synthesis.

**1. Three delivery surfaces, not one channel — all fed by the same canonical report data, all private/Airat-only.**
- **Telegram (a channel, not a DM)** — short, punchy, phone-format: a handful of key facts/numbers, no long-form prose. Push-triggered.
- **Email** — the detailed, full HTML-equivalent rendering of the canonical report (Ticket 04's Markdown shape rendered richer). Push-triggered.
- **Local web page (localhost today)** — the same detailed content as email, viewed on demand on Airat's own machine (a big-screen scroll-through-and-formulate-a-follow-up-question surface). Pull-only, no schedule, always reflects the latest run.

Both push channels and the pull surface stay **strictly private** — single recipient (Airat), no public exposure. Airat considered hosting the underlying page on his own domain/VPS later, purely for uptime (a VPS stays reachable when his own machine is off — not for public visibility), and confirmed explicitly this stays closed/private even if it moves to a VPS. Because nothing here is public or redistributed, **Ticket 05's revisit condition (legal-designation labels become gates once publishing is in scope) is not triggered** — Ticket 05's policy stands unchanged. Revisit only if/when Airat actually wants an outside reader.

**2. Single daily schedule, no intraday path.** One regimen run (`send_allowed` computed once per day) triggers both push channels at the same time — no independent per-channel timers. No real-time/mid-day alert in v0 even though Telegram's immediacy would suit one (see Forward Note above — deferred to v1+, needs a cadence the collector doesn't have yet).

**3. Delivery decision is a 3-level, metric-driven function — not a flat A/B/C choice.** Grounded in fields the pipeline already computes (`send_allowed`, `block_reason`, `article_count` from `scripts/news_pressure_regimen.py`; the 5-non-noise-type classification from Ticket 02; severity-b `needs_review` flags from Ticket 03; the zero-signal Technical/Ops trigger from Ticket 04):

```
def delivery_decision(run):
    if not run.send_allowed:                          # Level 1 — Ticket 03 hard blockers
        return telegram_only(block_ping(run), dedupe_if_same_reason_as_yesterday=True)

    if run.signal_cluster_count == 0:                  # Level 2 — Ticket 04's Technical/Ops trigger
        if not should_notify_needs_review(run.needs_review_count):   # Level 3 — black-box, threshold K from registry
            return telegram_only("Без сигналов. {N} кластеров — шум.")
        else:
            return telegram_only(needs_review_ping(run))             # Ticket 03: soft-blocks must never go silent
        # local page always regenerates with the full Technical/Ops report regardless of this branch

    return telegram(short_digest) + email(full_report) + local_page(full_report)   # normal product day
```

Key points behind this function, each traceable to a specific principle already established elsewhere in the project rather than invented fresh here:
- **Level 1** reuses Ticket 03's "never silent" rule and the dedup decision below (3a) — a hard block always pings Telegram, but repeats of the same `block_reason` compress into a short "still blocked, day N, same reason" instead of a full repeat.
- **Level 2** is exactly Ticket 04's existing Technical/Ops trigger (no cluster reached any of the 5 non-noise canonical types) — not a new definition, just wired into the delivery function.
- **Level 3 is the one genuinely new finding this session surfaced**: on an all-noise day, Ticket 03's severity-b `needs_review` clusters would otherwise have nowhere to surface (they normally live in the *product* report's "needs review" section per Ticket 04 decision #4, which isn't sent on a zero-signal day). Left unhandled, this would silently violate Ticket 03's "no block, hard or soft, may ever be silent" rule. Resolved: a distinct, richer Telegram ping fires whenever `needs_review_count` clears a threshold, even on an otherwise-quiet day.
- `send_allowed` and `signal_cluster_count == 0` are **structural existence checks**, not tunable thresholds — there's no meaningful alternative to "zero," so neither goes in a parameter registry.
- The `needs_review_count` threshold (`K`, default value `1` — "any flag is enough") **is** a genuine tunable and is treated as one: it lives behind a black-box function (`should_notify_needs_review`), sourced from Ticket 02's versioned parameter registry rather than hardcoded inline, with `K=1` as a concrete non-stub starting value (Ticket 02's "no empty modules" principle) explicitly flagged provisional. Recalibrating `K` from real accumulated noise/flag history is **Ticket 11's** future "recalibrate from the system's own operational logs" mechanism (forward-noted there this session) — distinct from Ticket 12's one-time historical bootstrap, which runs before the system has any operational history of its own.

**4. Content stays text-only on every channel at v0.** No charts, no color/visual styling anywhere — including Telegram, despite Airat's original instinct toward "maybe some neat graphics." A working chart-generation pattern exists elsewhere in the repo (see the Editor forward note above) but designing digest-specific charts is left as a distinct, later implementation decision, not solved here.

**5. Recipient storage: a config file today, behind a stable access interface, not a DB registry.** A single small config (e.g. `config/delivery.yaml` or equivalent) holds the Telegram channel ID and email address for v0's one recipient (Airat). All consuming code reaches it only through one function (e.g. `get_recipient(channel)`) — never a literal ID inline anywhere else in the codebase. This directly applies the already-standing "stable interface, swappable internals" principle (`idea-buffer.md`, 2026-07-31): today the function reads a file; if a second real recipient ever appears, only the function's internals change to read a DB table instead — no caller code changes. A full DB registry table (matching the Ticket 10 instrument-registry pattern) is explicitly not built now — it would be premature scaffolding for a single-user product whose "multi-user?" question `map.md` itself still leaves open under `## Not Yet Specified`.

**6. Multi-locale/multi-audience rendering — reconfirmed out of scope, no change from Ticket 04.** Still Airat-only, still Russian-only; revisit only if an external audience ever becomes real.

**7. Legal-designation revisit (Ticket 05) — reconfirmed not triggered.** See point 1 above; Ticket 05's policy (labels, not gates, while private/non-redistributing) stands unchanged.

## Grilling Transcript

Full record of the `/grilling` session behind the Working Decision above, per the convention on `docs/agents/issue-tracker.md`. Background research before the session confirmed factually that no Telegram/email delivery code exists anywhere in the repo yet (pure greenfield decision territory), that `send_allowed`/`block_reason` already exist as computed fields in `scripts/news_pressure_regimen.py`'s `regimen_runs` table, and that the collector only supports `daily/weekly/monthly/history` modes — no intraday/hourly mode.

**Q1 — Channel count for v0: one or several?**
Claude recommended a single channel (Telegram DM only) for v0, citing "don't build for a hypothetical" and technical simplicity, with email parked as an explicit backlog candidate.
Airat pushed back with a fuller picture: creating a Telegram channel for a test is easy, he already has channels he can use — but the small phone window needs real thought about format (short, catchy, maybe simple graphics, "the essence"). He also wants email, and introduced a new idea: a domain/VPS-hosted page that could double as a project "business card," with an agent swapping its rendered content, reused as the source for the email send. He was explicit these should be **two genuinely different formats from the same underlying data** — Telegram is not just a shorter email.

**Q2 — Is the domain/business-card page meant to be public, or private infrastructure?**
Claude flagged this as a real conflict before answering it: a public page would collide with Ticket 01's solo/private product scope and with Ticket 05's explicit note that legal-designation labels are a private-use-only policy, due for revisit once publishing enters scope (i.e., possibly right now, if this page is public). Recommended keeping it private/unlisted for v0.
Airat confirmed unambiguously: **"да конечно это приватная тема это 100%"** — today everything runs on localhost, purely so he can view a nice report on a big screen; himself the only audience. He clarified the VPS idea was only about uptime (a VPS stays reachable when his own machine is turned off), not about visibility, and confirmed it would stay closed/private even if moved to a VPS later: **"хорошо но это понятно мы ничего не нарушаем."** This also folded in his earlier point: email content and the local page are the same detailed rendering, viewed two different ways (pushed vs. pulled), while Telegram stays a structurally different, condensed format — matching the pre-existing Editor-role forward note from Tickets 04/10 almost exactly.

**Q3 — Do Telegram and email share one schedule, or run independently?**
Claude recommended a single schedule: one daily regimen run triggers both push channels simultaneously, with the local page as a pure pull artifact with no schedule of its own.
Airat agreed: **"да. одинаковое. источник один и тот же. поэтому одинаковая рассылка."** He added a nuance that opened Q3a: because Telegram is more "operational" (he and people generally check it more often), it's well suited to reporting something that "broke through" quickly.

**Q3a — Does that nuance mean a new intraday/real-time alert path, separate from the daily schedule?**
Claude checked the collector code first (confirmed only `daily/weekly/monthly/history` modes exist, no hourly mode) and framed two readings: (A) it's just a justification for Telegram-as-primary-channel, no new mechanism; (B) it's a request for an actual mid-day alert trigger outside the daily cycle. Recommended (A) — stay on the single daily schedule, defer real intraday alerting as a v1+ idea, since it would need new infrastructure (a more frequent detection cadence, a spam-guard) not justified before the daily loop itself is proven.
Airat chose (A) explicitly: **"давай в рамках прототипа будем как написал. никакого оперативного не будем делать. вопрос закрыт. как планировали так и делаем."** Recorded as a Forward Note on this ticket for v1+.

**Q4 (opened, then restructured into a tree at Airat's request) — Failure/blocked-run notification behavior.**
Claude initially proposed a single combined recommendation (always notify via Telegram only, with reason+severity+counts), citing the conflict between this ticket's own Starting Assumption ("send blocked-status only after repeated failures") and Ticket 03's already-decided, stronger "never silent" rule (the latter wins, since it was decided later and is more specific).
Airat asked to slow down and build this as an explicit branching tree first, deciding node by node: **"давай по логике это разрулим. давай пока есть возможность раскроем эти варианты и по дереву решений тогда примем что сделать."**

- **Node 1 (trigger — always vs. debounced vs. threshold-gated vs. silent-until-asked):** Claude laid out options A (always, full message every time) through D (silent until asked), ruling out C/D as contradicting Ticket 03's never-silent rule, and recommended B (always notify, but compress repeats of an unchanged reason into a short form). Airat: **"согласен."**
- **Node 2 (channel — Telegram / email / both / separate ops channel):** Claude recommended Telegram-only, since email is reserved for full-length content and a separate ops channel is unjustified extra surface area for a rare event. Airat: **"согласен."**
- **Node 3 (content — bare one-liner / reason+severity+counts / full Technical/Ops report):** Claude recommended reason + severity (Ticket 03's hard/soft distinction) + a brief collected-count metric, rejecting the bare one-liner (too little context) and the full Technical/Ops report (built for a different case — data present, no signal — and disproportionate to render when the pipeline may itself be partially broken). Airat: **"согласен."**

**Q6 — Multi-locale/multi-audience (Ticket 04's deferred item): still out of scope here?**
Claude asked directly whether anything about deciding real channels changes this. Airat: **"согласен и все ок"** — no change, stays deferred.

**Q7 — Legal-designation revisit (Ticket 05's stated trigger condition): does it fire now?**
Claude argued no, since Q1/Q2 already re-confirmed everything stays private/single-recipient — the trigger condition (publishing enters scope) has not occurred. Airat: **"да все хорошо."**

**Q8 (opened, then deepened into a 3-level metric-driven tree at Airat's request) — What happens to the Technical/Ops report (all-noise day) at the delivery-policy level, which Ticket 04 explicitly left open?**
Claude first offered a flat A/B/C choice (deliver normally / never deliver / short Telegram-only ping), recommending C.
Airat asked for real technical depth instead of a flat choice: **"давай подробнее. техничнее основываясь на механизме. на метрике. как система должна принимать решение... вертка решений в глубину до 3 уровней... не бинарный."** Claude rebuilt the answer as the 3-level function in `## Working Decision` point 3, grounding each level in an already-computed pipeline field (`send_allowed`, `signal_cluster_count` from Ticket 04's own trigger definition, `needs_review_count` from Ticket 03's severity-b flags) rather than inventing new state. This surfaced a genuine gap mid-derivation: severity-b `needs_review` clusters have no home to surface in on an all-noise day (their usual home, the product report's dedicated section, isn't sent that day), which would silently violate Ticket 03's never-silent rule if left unhandled — resolved by making that case (3b) its own richer Telegram ping. Airat accepted the whole tree: **"ну хорошая прагматичная схема"** — then asked a sharper follow-up about the 3a/3b boundary itself.

**Q9 — Is the `needs_review_count` boundary (used to trigger the Level-3 richer ping) a hardcoded existence-check, or should it be a tunable, empirically-set parameter?**
Airat: **"экспертность только должна подставляться верно? из метрик которые каждый день могут быть разные. или как считаешь хардкод тут?"** Claude distinguished two categories — structural existence-checks (`send_allowed`, `signal_cluster_count == 0`, both already fixed by Ticket 03/04's own definitions, no registry needed) from genuine tunable thresholds (a real alternative value could someday be justified) — and identified the needs-review boundary as belonging to the second category if Airat wanted room to suppress single-flag pings later. Recommended `K=1` (existence-check, no registry) as the default, deferring calibration until real operational data justifies a change, consistent with Ticket 12's "don't guess numbers" precedent.
Airat went further than either option offered: **"давай сделаем это как черный ящик функцию. проверяет те и те значения. ну и заполнить это значение каким-то показателем, который мы выясним из эмпирики. из нашего прогона."** Claude reconciled this with Ticket 02's "no empty modules" principle (a concrete, non-stub starting value is still required — `K=1`) and Ticket 11's already-recorded, not-yet-resolved "recalibrate from the system's own operational logs" forward note (the natural future owner of tuning `K`, distinct from Ticket 12's one-time historical bootstrap). Airat confirmed by moving the session forward: **"давай дальше. разбираем все подробно. фиксируем то что на сейчас есть."**
