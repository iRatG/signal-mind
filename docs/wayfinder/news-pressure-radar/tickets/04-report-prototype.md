# Ticket 04 - Report Prototype

Status: closed
Type: prototype
Labels: `wayfinder:prototype`
Claim: claimed by Claude (wayfinder session, 2026-07-31)
Blocked By: Ticket 01 - Product Contract, Ticket 02 - Signal Ontology (both closed 2026-07-31 — this ticket is now unblocked)
Blocks: Ticket 07 - Delivery Policy, Ticket 08 - OpenSpec Bridge

## Question

What should the first excellent News Pressure Radar report look like?

## Why This Matters

The report is the product surface. Before dashboard or delivery automation, Airat needs a format that is quick to scan and trustworthy enough to read repeatedly.

## Prototype Scope

Create one concrete report prototype in Markdown with:

- title and period;
- executive summary;
- main pressure signal;
- rising signals;
- synchronized stories;
- one-source anomalies;
- noise section;
- source/evidence links;
- watch-next list;
- quality status block.

## Decision Shape

The ticket resolves when Airat accepts one report shape as the target for OpenSpec.

## Forward Note (2026-07-31, captured while grilling Ticket 01, not resolved)

Airat sketched a three-section daily structure, richer than the Prototype Scope list above — reconcile the two when this ticket is actually claimed:

1. **General** — what happened in the news, which index/instrument looked more correlated or volatile. Terminology resolved 2026-07-31 below (canonical↔human dictionary).
2. **Process/technical** (decided 2026-07-31: lives OUTSIDE the daily product report — a separate internal log, candidate home `db/knowledge.md`, not a report section) — which of *our own* metrics fired or misfired this run, an evolutionary log of what's kept vs dropped in the method itself. This is Ticket 11's audit/iterate loop, deliberately kept out of the product surface so the daily report stays about news/market, not method-navel-gazing.
3. **Overall progress** — resolved 2026-07-31 below: moves out with #2, into the new Technical/Ops report mode rather than the daily product report.

Weekly cadence (once added, see Ticket 01) could add a word-cloud / most-frequent-terms view across the week on top of the daily structure.

## Forward Note (2026-07-31, captured while grilling Ticket 10 - Market Coupling Model, not resolved)

Airat introduced a distinct **Editor role**: whatever renders the final human-facing output (report, Telegram post, email) is a separate block from whatever computes the underlying analysis — same role-independence principle just recorded on [Ticket 11](11-agentic-delivery-process.md) and [[idea-buffer]], applied to presentation instead of research roles. Hard constraint on this role: it must never invent or paper over missing data — if a data point is unavailable, it says so explicitly rather than filling the gap with plausible-sounding prose. This is a "block system that checks itself" — the Editor consumes only what upstream blocks (Runner/Auditor/Synthesizer, or the daily pipeline's own signal detection) explicitly hand it, never fabricates evidence on its own.

Airat sketched two output surfaces with different depth, both owned by this Editor role: a short, visually polished Telegram post with graphics/charts (subject to Ticket 07's delivery policy — Telegram is currently disabled pending that decision), and a more detailed HTML email page with its own charts and fuller analysis. He explicitly named tone, color, seriousness, and date/dateline formatting as things this role should get right — "it's the face of the project." Not decided yet: exact chart types, color/tone guidelines, or how HTML-email and Telegram-post content should differ beyond length. Resolve when this ticket (and Ticket 07 for the delivery-channel split) is actually claimed — the `dataviz` skill is a candidate tool for the chart/color decisions once this reaches that stage.

**Scope confirmed 2026-07-31, while resolving this ticket:** everything in this note stays out of Ticket 04's Markdown-only prototype and is explicitly Ticket 07 territory. Two more items were added to it during Ticket 04's grill, both explicitly deferred here rather than decided:
1. **Multi-locale/multi-audience rendering.** Airat generalized the canonical↔human dictionary idea (see `## Working Decision` below) beyond language: the *canonical* vocabulary should stay market/audience-independent, but the *human-facing* rendering could vary by audience — e.g. describing the Russian market to a foreign investor might need different framing entirely, not just translation. Out of scope while the product stays solo/Airat-only (Ticket 01) — becomes relevant only if/when the project ever serves an external audience.
2. **"Hook in 3 seconds" tone + visuals.** While discussing the Executive Summary, Airat asked for the report to grab attention immediately — professional, polished, "marketing" in impact but explicitly *not* fake, salesy, cheap, or sensational — plus "a beautiful chart." The chart/color/visual-polish half of this is Editor/Ticket 07 scope (no charts or color in a Markdown file); Ticket 04 addressed only the text-content half (see the Executive Summary decision below).

**Resolved 2026-07-31 (Ticket 07's own grilling session):** Editor role is now split into three delivery surfaces (Telegram/email/local page) sharing one canonical report; all three stay text-only at v0, so item 2's chart/visual half is deferred again, now to actual rendering-implementation time. Item 1 (multi-locale) was reconfirmed out of scope, unchanged. See [Ticket 07 - Delivery Policy](07-delivery-policy.md) for the full decision.

## Working Decision

Resolved 2026-07-31 (`/grilling` session). Scope agreed with Airat before grilling started: this ticket resolves only the canonical Markdown report shape (the single source-of-truth structure and field set) — the Editor/multi-channel rendering question (Telegram vs. email, tone, color, charts) is explicitly deferred to Ticket 07, not decided here.

An important process correction surfaced mid-session: the existing report-writer code (`analytics/news_pressure_cluster.py`, `write_digest_v2`) is a spike/prototype, not a structural constraint. It was used only to confirm what data already exists (fields, computed metrics, e.g. `article.url`) — not as a default shape to preserve. The actual shape below comes from already-closed tickets (01/02/03/05/10) plus this session's live decisions; where a prior ticket had already answered a question, that answer was reused rather than re-litigated.

**1. Canonical↔human dictionary for section headers.** A two-column table — canonical signal type (Ticket 02) ↔ current human-facing Russian label — becomes the single source of truth that the report renderer reads from, instead of hardcoding label strings per mode:

| Canonical type (Ticket 02) | Human-facing label (current, Russian) |
|---|---|
| Main pressure | Главный импульс дня / недели |
| Rising impulse | Что вспыхнуло / ускорилось |
| Persistent background | Устойчивый фон |
| Synchronized story | Синхронно подхватили |
| One-source anomaly | Один источник — аномалия |
| Noise/routine | Шум/рутина (not a narrative section — see below) |
| Needs review (Ticket 03 severity-b) | Требует проверки |

Multi-locale/multi-audience variants of the human column (a different language, or a different framing for a non-Russian audience) are explicitly out of scope — forward-noted above and in `idea-buffer.md`.

**2. One-source anomaly vs. Noise/routine — applying Ticket 02, no new decision.** One-source anomaly gets a real narrative section. Noise/routine gets no narrative section at all — just a one-line audit count ("N рутинных/неподтверждённых кластеров исключено").

**3. New: Report Mode Split — Product report vs. Technical/Ops report.** When a run clears Ticket 03's hard blockers (articles and clusters exist, no collector failure) but **no cluster reaches any of the 5 non-noise signal types** (an all-noise day), the system does not send a product report. It sends a **Technical/Ops report** instead — visually distinct from a product report (exact visual treatment is Ticket 07/Editor scope) — stating plainly that the run completed normally but found nothing reportable. This is a new, distinct outcome from Ticket 03's existing hard block (which covers data *absence*, not data-present-but-no-signal). Ticket 01's previously-undecided "Overall progress" section (a meta read on system functionality, not market content) lives in this Technical/Ops report, alongside the Process-technical journal content already decided to live outside the product report. Both report types are Airat-only at this stage; no external delivery implication.

**4. Quality data placement — per-cluster findings stay in the product report; run-health metrics do not.** Ticket 03's "needs review" list (severity-b clusters: `single_source_persistent`, `single_source_high_volume`, `llm_fallback_heuristic`) stays visible in the product report as its own section — it's a specific finding worth Airat's attention, not a system-health metric. Run-level health metrics (`send_allowed`, `article_count`, `quality_flag_count`, `source_counts`, etc. — already computed by `save_status()`) do **not** get a dedicated product-report section; they belong to the ops/technical log level instead (see the new three-tier framing below).

Airat additionally sketched a three-tier framing for metrics/logging generally, which goes beyond this ticket's report-shape question: an **ops/technical log level** (run happened, data collected, technical logs), a **metrics/decision level** (data run through the project's actual analytical approach — clustering, scoring, Ticket 03's severity checks — producing the judgments that decide whether anything is reportable), and a **product level** (only produced if the metrics level yields something with real analytical/business meaning). This three-tier framing is recorded here for context but is not itself a Ticket-04 deliverable — the concrete tiers above (product report / Technical-Ops report / `db/knowledge.md` journal) already instantiate it for this ticket's scope.

Two items Airat raised while discussing this were routed elsewhere rather than decided here: a non-auto-fixing diagnostic/support-agent role that files a ticket instead of patching code when something breaks (reinforces an existing forward note on Ticket 11), and a general principle that any fix, at any tier, must follow the project's documented concept/spec rather than an ad-hoc patch (new `idea-buffer.md` entry). Both are recorded on Ticket 11 and `idea-buffer.md`, not here.

**5. Evidence links — new rendering of an existing field.** `article.url` is a real, already-populated field (confirmed in code, e.g. `analytics/news_pressure_cluster.py:323`) but was never rendered into the report text before this decision. Each signal in the product report now lists its top representative headlines as markdown links: `[Заголовок](url) — Источник, дата`. Justification beyond reader convenience: this also gives Ticket 06 (Evaluation Harness) and Ticket 12 (historical calibration) a concrete evidence trail to retrospectively verify whether a reported signal actually held up, instead of relying on an LLM's paraphrase.

**6. Watch-next aggregation — attributed list, not blended prose.** The per-cluster `watch_next` field (one sentence per cluster) is aggregated into the "Что смотреть дальше" section as a list, each item tagged with the signal it came from (e.g. "— [Главный импульс] следить за реакцией ЦБ на этот вопрос") — not summarized into one LLM-blended paragraph. Consistent with Ticket 02's Starting Assumption: interpretable evidence over rhetorical polish.

**7. Executive Summary — a fact-based lead sentence, then a bullet nav list.** The summary opens with one sentence naming the single most notable finding of the period using a concrete fact or number (e.g. "Сегодняшний главный импульс — «X» с 12 источниками за 6 часов, самый быстрый рост за 2 недели") — never an evaluative adjective ("невероятно", "срочно") standing in for a fact. Below that lead sentence, a short bullet list gives one line per non-empty section with its count (вспышки: N, синхронные истории: N, требует проверки: N), so the reader can decide whether to read further within seconds. Airat asked for the report to "hook" a reader immediately, professional and polished but explicitly not fake/salesy/cheap/sensational — the fact-first lead sentence is how this ticket's Markdown-only scope satisfies that; visual/color/chart polish is deferred to Ticket 07 (see forward note above).

## Grilling Transcript

Full record of the `/grilling` session behind the Working Decision above, per the convention on `docs/agents/issue-tracker.md`. A background research pass (`Explore` agent) first confirmed the real state of `analytics/news_pressure_cluster.py`/`scripts/news_pressure_regimen.py` before grilling started, so every recommendation below reflects what the code actually computes today, not a guess — though, as the transcript shows, Airat corrected the *use* of that grounding partway through (facts about existing data are useful; the existing code's report *structure* is not a constraint).

**Section naming (canonical types vs. current human labels):**
Claude recommended keeping the current friendly Russian section labels as-is, with Ticket 02's canonical types as an internal-only tag, to avoid re-labeling a report that already reads well.
Airat pushed further: he wants an actual **dictionary** ("словарь") — technical term to human term — because "не все профи или терминология разная." Claude proposed a two-column table as the single source of truth for rendering. Airat then generalized once more: if the project ever serves other markets, or explains the Russian market to a foreign investor, the internal/canonical vocabulary should stay fixed regardless of market, while the external/human rendering could be swapped per audience — "внутри одна своя терминология... а вот снаружи мы это конвертируем под клиента, у словно, мультиязычный продукт." Claude flagged this second point as Editor-role/Ticket 07 territory (Ticket 01 already scoped the product as solo, no external audience yet) rather than something to solve today. Airat agreed to build just the dictionary now: **"словарь да. делаем. просто пока словарь. остальное как на будущее. в todo."**

**One-source anomaly vs. Noise/routine split:**
Claude initially framed this as an open question requiring a fresh decision (two sections vs. one section with subheadings), grounding the framing in the current code's single conflated section.
Airat corrected the framing directly: **"не сейчас в коде. кода нет. это прототипим... мы пишем спецификацию... поэтому нужно уже исходить часто из того описания что мы уже сделали. и если уже из этого нашего описания нет деталей — непонятно как делать — то да, давай гриллим."** Claude re-checked Ticket 02's existing per-type decisions and found the answer already there (anomaly gets a narrative section, noise/routine gets only a quiet count) — not a new decision, just applying an old one. Airat confirmed the general rule this sets for the rest of the session: **"если нужны детали давай обговорим. если о них ничего не сказано. то давай это обсудим. если нет понимания. если есть туман."**

**"Overall progress" placement → new Report Mode Split:**
Claude asked the originally-planned question (does Overall progress stay in the daily report or move out with Process-technical), recommending it move out.
Airat answered by introducing a new mechanism unprompted: if the run went well and found something substantive, send the normal product report; if the run went well but found nothing ("шума нет, ничего нет"), don't send a product report at all — send something addressed to himself as "техническому персоналу службе поддержки 2-3 линии," marked visually differently (e.g. a different color) so it's never confused with a real product finding. Claude reflected this back, distinguishing it from Ticket 03's existing hard block (data-absence case) versus this new data-present-no-signal case, and asked for confirmation of that boundary. Airat confirmed: **"да. я думаю это логично."**

**Quality data placement (needs-review vs. run-health metrics):**
Claude proposed: Ticket 03's per-cluster "needs review" list stays in the product report (a specific finding); run-level health metrics (`send_allowed`, counts) move to the new technical/ops tier instead.
Airat confirmed emphatically — **"Да да да все верно это разные уровни"** — and then dictated a fuller three-tier picture unprompted: a technical/ops log level (raw run execution, data collected, technical metrics), a metrics/decision level (data run through the project's actual method, producing judgments), and a product level (only produced if the metrics level yields real business/analytical meaning). He also introduced, in the same breath, a non-auto-fixing diagnostic agent that files a support ticket (to himself, "2-я линия поддержки") when a module breaks rather than patching it, and a rule that any fix at any level must follow the documented concept, never an ad-hoc patch ("чинить будем в соответствии с концепцией, чтобы не было заплаток"). Claude routed the diagnostic-agent and no-ad-hoc-patch points to Ticket 11 and `idea-buffer.md` (both project-wide, not report-shape questions) rather than deciding them here, which Airat accepted when asked directly: **"да, полностью согласен и понял... фиксирую сейчас же."**

**Needs-review visibility (closing the loop on the quality-data question):**
Claude asked directly whether severity-b clusters (Ticket 03) stay visible in the product report's "needs review" section as already decided, or get demoted further given the new three-tier framing — recommending keeping Ticket 03's decision as-is.
Airat agreed without reopening it: **"давай давай оставим пока есть."**

**Source/evidence links:**
Claude verified in code first (not asked as a guess) that `article.url` is real and already populated, but unused in the report today, then proposed markdown links per representative headline under each signal.
Airat conditioned his agreement rather than approving outright: **"ну если мы с этого поимеем какую аналитику статистику и нам это поможет то да. я согласен и если это не дорого по реализации."** Claude confirmed both conditions explicitly — cheap (the field already exists, this is a rendering change, not new data collection) and useful beyond convenience (gives Ticket 06/12 a concrete trail to retrospectively verify a reported signal against its source, rather than trusting an LLM paraphrase). Airat did not need to respond further before the session moved on — the conditions being satisfied stood as agreement, confirmed implicitly by his continuing to the next question.

**Watch-next aggregation:**
Claude offered two options: an attributed list (one item per signal, tagged with its source signal) or blended LLM prose with no per-signal attribution, recommending the attributed list per Ticket 02's interpretable-evidence principle.
Airat: **"согласен."**

**Executive Summary:**
Claude first proposed a pure bullet-count navigation list with no lead text, grounded in Ticket 01's "quick to scan" language.
Airat pushed for more: **"важно зацепить в начале... важно что-то интересное... это маркетинг... но и не фальшиво и не продажно и не дешево и не скандально. но очень профессионально... и график красивый... правдиво. аккуратно. строго."** Claude split this into two halves: the chart/color/visual-polish half is Editor/Ticket 07 scope (no charts in a Markdown file); for this ticket's text-only scope, added a fact-based lead sentence above the bullet list (a concrete number/fact, never an evaluative adjective standing in for one) as the way to "hook" a reader without becoming salesy. Airat confirmed: **"да. хорошо. главное профессионально. красиво. аккуратно. и на наших супер проверенных данных и подходе."**
