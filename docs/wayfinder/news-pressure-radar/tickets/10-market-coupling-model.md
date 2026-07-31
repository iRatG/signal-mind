# Ticket 10 - Market Coupling Model

Status: closed
Type: grilling
Labels: `wayfinder:grilling`
Claim: claimed by Claude (wayfinder session, 2026-07-31)
Blocks: Ticket 06 - Evaluation Harness, Ticket 08 - OpenSpec Bridge

## Question

What first form of relationship should News Pressure Radar measure between news pressure and market data?

## Why This Matters

Airat's clarified goal is not trading prediction. The goal is digitizing the world: people speak, events happen, news pressure forms, and market indices/currency rates leave measurable traces. The pilot needs a narrow model of this relationship before implementation.

## Decision Shape

Choose the first coupling model:

- event window association: news cluster on date D, market trace on D, D+1, D+3, D+5;
- event window association around the event: D-3, D-1, D, D+1, D+3, and optionally D+5;
- market-first backtrace: instrument anomaly on D, then inspect news pressure on D-3..D;
- abnormal move detection: movement versus usual volatility or broad market baseline;
- sector sensitivity table: which topic types tend to align with which indices;
- pair signal discovery: when one index or currency moves first, test whether another tends to move after it;
- explanation layer: LLM explains only after the numeric relation is computed.

The answer must define:

- which market instruments are in the pilot;
- what windows are used;
- what counts as a "trace";
- what source-spread/volume threshold qualifies the news side as meaningful;
- whether the first spike should run news-first, market-first, or both directions;
- whether pairwise index/currency signals are in the first pilot or the second stage;
- which text depth is required at each stage: headline, description/lead, or full article;
- how to avoid claiming causality too early;
- what a useful output row looks like.

## Starting Assumption

Start with an event-study style table: `event_signature -> instrument -> return_-3d/-1d/+1d/+3d/+5d -> abnormality -> relative move -> short explanation`.

## Working Decision

Grilled from a clean slate on 2026-07-31, deliberately ignoring the pre-existing 2026-07-28 draft (`openspec/changes/news-market-coupling-agentic-loop/design.md`, mini-spec 01, runner-brief 01) as a source of default answers, per Airat's explicit instruction — see this ticket's Grilling Transcript for the live Q&A. Where the fresh answer converges with that older draft, treat it as an independent confirmation, not inheritance. The older draft is reconciled against this decision separately (see the OpenSpec change's own updated files).

**1. Instrument universe — registry-driven, not hardcoded.** Market instruments are rows in a reference table, never literals inside code logic (`if symbol == "IMOEX"` must not exist anywhere). Code iterates over registry rows filtered by an `active`-style flag; it never branches on a hardcoded name. This generalizes Ticket 02's "versioned parameter registry" principle from thresholds to entities — recorded project-wide in [`idea-buffer.md`](../idea-buffer.md).

The registry is seeded from the full MOEX ISS API candidate list already confirmed live (`/iss/engines/stock/markets/index/securities.json`, `/iss/engines/currency/markets/selt/securities.json`): `IMOEX`, `RTSI`, 10 sector indices (`MOEXOG`/`MOEXFN`/`MOEXMM`/`MOEXCN`/`MOEXEU`/`MOEXTL`/`MOEXTN`/`MOEXCH`/`MOEXIT`/`MOEXRE`), 3 FX pairs (`USD000UTSTOM`, `EUR_RUB__TOM`, `CNYRUB_TOM`). All of these are loaded into the table regardless of pilot status; an `active_in_pilot_v0` flag marks the actual v0 subset: `IMOEX` + a small number of the most news-sensitive sector indices (oil & gas, financials, as strong candidates; exact final set to confirm at implementation time against real data availability) + all 3 FX pairs. `RTSI` starts `active=false` — it largely duplicates `IMOEX` in dollar terms and would add noise without new signal in v0. Expanding to more instruments later is a data edit to this table, not a code change.

Data source confirmed live 2026-07-31: MOEX ISS API (`iss.moex.com`), public, no login/API key required, verified against `/iss/history/engines/stock/markets/index/securities/IMOEX.json` — real historical OHLC rows, same shape the existing manual-ZIP loader (`src/parsers/moex_indices.py`) already parses. This can replace the manual export-and-ZIP workflow.

**2. Direction — market-first for v0.** Start from a market anomaly on date `D`, then look at news pressure in the window around it. News-first and bidirectional analysis are deferred to a second wave.

Reasoning is a genuine resource asymmetry, not a compromise: market data (MOEX ISS API) is free, deep (years, one API call), and already validated. News data is the scarce resource — the 14-source RU set (Ticket 05) only went live 2026-07-31, and `db/hf_news.db`'s `ru_archive:` tail covers only the original 5 sources for roughly the last ~10 months. A news-first pilot would require a mature, calibrated clustering pass over continuous historical news (persistence/velocity signal types from Ticket 02 are inherently continuous-time constructs) — that calibration is Ticket 12's separate, not-yet-run job. Market-first instead needs only: cheap anomaly statistics on already-available market history, plus a **targeted** look at news only around the small number of anomaly dates found — which does not require Ticket 12 to have run first.

Confirmed technical fact (checked against `ru_news_archive_loader.py`, not assumed): the ~10-month depth of the existing RU archive is how far the loader has actually been run, not a hard ceiling. The loader scrapes day-indexed archive pages (`kommersant.ru/archive/news/day/{date}`, `interfax.ru/news/{date}`, `lenta.ru/{date}`; `vedomosti.ru/archive/{date}` is flagged flaky in the code) that exist for years back on the source sites themselves. So market-first's targeted historical news lookback for any anomaly date (not just recent ones) is achievable today for Kommersant/Interfax/Lenta via this loader, and for Vedomosti with a caveat. RIA (part of the original 5) and all 9 sources added in Ticket 05 have no adapter in this loader yet — extending it is real but modest follow-up work (same `BaseSource` pattern, 2 methods per source), not a blocker for v0, and not attempted this session.

**Backlog (stage 2, explicitly not forgotten):** pairwise index/currency signals (does one instrument moving tend to precede another?) and news-first/bidirectional analysis both wait until market-first validates the core hypothesis and Ticket 12 gives the news side a calibrated historical baseline. The point of the Runner→Auditor→Synthesizer loop (`design.md`) is exactly that testing either idea later is a new cycle brief through the same pipeline, not new bespoke code.

**3. Event window — `D-3..D+3`, `D+5` as an optional extended horizon, config-driven.** Symmetric window catches both pre-event leakage/rumor and post-event coverage. Like the instrument registry, window sizes (`window_pre_days`, `window_post_days`, `window_extended_days`) live in the same parameter registry as Ticket 02's thresholds — never a hardcoded `range(-3, 4)` in code.

**4. What counts as a "trace" (anomaly/abnormality definition) — a pool of several metrics, not one fixed rule.** A single metric was explicitly rejected as insufficient. v0 uses a small pool (candidates: top-N% absolute daily move over a quarter, deviation beyond a volatility band, relative move versus `IMOEX` for sector indices) evaluated as a **raw vector per date**, not a hard "N of M passed" gate — closer to a decision-tree/combination structure than a single threshold. Metrics may prove instrument-specific or even news-topic-specific; this is allowed, not a defect. The exact metric list, count, and combination logic are calibration/build work (likely intersecting Ticket 12), not finalized number-by-number in this decision ticket.

**5. Source-spread/volume threshold for "meaningful" news — descriptive, not a hard numeric gate at v0.** For every market anomaly, record source count, headline count, and same-day-multi-source sync as facts (same spirit as Ticket 05's `category_spread`), rather than setting a numeric cutoff now. Anomalies with weak or no visible news pressure are kept as explicit **negative cases**, not discarded — they are valuable sample, not noise. A numeric significance threshold, if one proves useful, is derived empirically later (Ticket 12), consistent with the project's standing "not from imagination, from data" principle (echoed from Ticket 05).

**6. Text depth — staged by default.** Headlines are the default corpus for the full scan. Descriptions/leads are used where already present in already-collected data (no extra fetch). Full article text is fetched only for a small number of candidates where a strong market anomaly has an ambiguous or uninformative headline. This matches the MVP's existing headline-first posture (Ticket 05).

**7. Empirical calibration is achievable, not aspirational.** Two real, working resources back this: `db/hf_news.db`'s existing `ru_archive:` data (queryable today, no new code) and `ru_news_archive_loader.py` (working, extensible scraper for targeted historical backfill). Initial coefficients — anomaly-metric thresholds, any future news-significance cutoff — are meant to be derived from these once real runs accumulate data, not picked from imagination, matching the standing project principle already set on Ticket 05/12.

**8. Avoiding premature causality claims — inherited, not a new decision.** Language rules ("coincides with", "candidate relation" — never "caused"/"proved"/"predicts") and the Auditor's review checklist (lookahead leakage, cherry-picking, missing negative cases, overclaiming) are already fixed project-wide by Ticket 01's retrospective-only framing and `design.md`. This ticket adds nothing new here.

One real open sub-question surfaced and was deliberately **not** resolved here: whether the Auditor is a separate agent instance from the Runner (`design.md`'s own unresolved "Open Question"). Airat sharpened the reasoning — role independence is non-negotiable (an agent must never audit its own work), generalized into a project-wide principle that the *interaction contract* between modules/roles stays fixed while internal logic is free to change. This belongs to **Ticket 11 - Agentic Delivery Process** (which owns "separate agents vs. one agent with review phases"), not to this ticket — recorded there as a forward note, plus a general entry in `idea-buffer.md`.

**9. Output row — the Runner's raw evidence record** (distinct from any human-facing rendering, which is Ticket 04/07's Editor-role territory, also captured as a forward note this session):

```
instrument_id, event_date, window (D-3..D+3, optional D+5),
metric_vector (which anomaly metrics fired + raw values),
news_source_count, news_headline_count, news_sync_flag,
news_peak_timing (day within the window where news volume peaked),
relation_note (free text, no causal language),
negative_case (bool — anomaly with no clear news trace)
```

## Grilling Transcript

**Q1.** Which market instruments go into the v0 pilot? Recommendation offered: a narrow core (`IMOEX` + 3-4 news-sensitive sectors + all 3 FX pairs), `RTSI` inactive.
Airat's answer: agreed it's a pilot, but insisted the instrument list must never be hardcoded — "нигде в коде не должно быть явно прописано moex rti usd... это прописано только в таблицах настройки. система должна быть беспощадна, холодна, рациональна." Confirmed the full candidate universe should live in a registry table (sourced from MOEX ISS API), with the pilot subset as just a flag on that same table, so scaling up later is a data edit, not new code.

**Q2.** Direction — market-first, news-first, or both? Recommendation offered: market-first, deferring the others to stage 2, framed initially around "we don't have deep RU news history yet."
Airat pushed back on the framing: "мы это должны заказать... у нас есть база какая-то, разве не можем забрать оттуда обучающие данные? не пугай меня." Checked `ru_news_archive_loader.py` directly rather than assuming — confirmed it scrapes day-indexed archive pages that exist for years on the source sites (not capped by how far the loader happens to have been run), for Kommersant/Interfax/Lenta reliably, Vedomosti with a caveat; RIA and the 9 Ticket-05 sources have no adapter yet. Revised recommendation: market-first still wins, but because it only needs *targeted* backfill for a handful of anomaly dates, versus news-first needing a full continuous historical run across all 14 sources before any clustering signal is even meaningful. Airat confirmed: yes, and confirmed empirical calibration (not "из головы") is achievable via `hf_news.db` + the loader.

**Q3.** Event window shape? Recommendation offered: `D-3..D+3` with optional `D+5`.
Airat's answer: agreed, with the same registry-not-hardcode requirement applied to window sizes as to instruments — "тоже фиксируем как конфигурационный показатель... чтобы в коде была разбивка по разным показателям, которые задали в словаре... гибкость, не хардкод."

**Q4.** What defines an anomaly/"trace"? Recommendation offered: a small pool of 2-3 metrics evaluated as a raw vector, not one fixed rule.
Airat's answer: "абсолютно точно нужно иметь несколько метрик... должна быть некое дерево решений... мощностей хватит... может быть оно в итоге для разных сущностей... будет разным" — confirmed multi-metric pool, open to a decision-tree-style combination and per-instrument/per-topic specialization, without pinning the exact list now.

**Q5.** What news-side threshold counts as "meaningful"? Recommendation offered: descriptive labels (source count, sync flag) rather than a numeric cutoff at v0; keep no-news cases as explicit negative examples rather than discarding them.
Airat's answer: "да, это мудро... будет пересматриваться когда у нас будут данные... главное это фиксировать, чтобы потом... было понятно, от чего мы приняли то или иное решение" — confirmed, and reinforced why the Grilling Transcript convention itself matters (future Auditor/reviewer needs the reasoning, not just the conclusion).

**Q6.** Pairwise index/currency signals — v0 or later? Recommendation offered: defer to stage 2.
Airat's answer: "в бэклог поставим. но не забудем... универсальный механизм, который прогоняет идею по нашему конвееру... по нашей фабрике проверки идей... сможем безболезненно любую идею проверить" — confirmed deferral, framed explicitly around the Runner/Auditor/Synthesizer loop being reusable for any future hypothesis, this one included.

**Q7.** Text depth per stage? Recommendation offered: headline default, lead if already present, full text only for a small number of ambiguous strong-anomaly candidates.
Airat's answer: "да, молодец что предложил. это грамотно." Confirmed without changes.

**Q8.** Does avoiding premature causality claims need a fresh decision here? Recommendation offered: no — Ticket 01 + `design.md` already fix the language rules and Auditor checklist project-wide; this ticket just inherits them.
Airat's answer: asked to double-check honestly rather than assume — "если мы что-то не расписали... давай проговорим." On inspection, found one genuinely open sub-question (`design.md`'s "is the Auditor a separate agent?") and surfaced it rather than silently closing it. Airat then generalized the reasoning far beyond this ticket: a checker must never check its own work (his example: coder and tester must not be the same role, "он сам себя будет прекрывать"), and more broadly, "у нас проект из модулей, мы меняем внутри модуля что-то или логику, но не меняем взаимодействие" — the interaction contract between modules/roles stays fixed, internals stay free to change. Agreed this open sub-question routes to Ticket 11, not this ticket.

Mid-session tangent, captured but not part of this ticket's decision: Airat raised a news-editor role for rendering final output (Telegram post with graphics, detailed HTML email, tone/color/seriousness) — recognized as Ticket 04/07 territory (presentation/delivery) rather than this ticket's raw evidence-row question, and recorded there as a forward note.

**Q9.** Output row shape? Proposed the schema in Working Decision item 9 above, framed explicitly as the Runner's raw evidence record, distinct from the Editor's future rendering.
Airat's answer: "все хорошо." Confirmed as-is, no changes.

Also confirmed mid-session (not tied to one specific question): a general "idea buffer" convention is needed for tangents that don't map to one ticket — created `docs/wayfinder/news-pressure-radar/idea-buffer.md` this session per Airat's request ("главное чтобы ты понимал куда это поместить... если к тикету не относится, просто записать это точно").
