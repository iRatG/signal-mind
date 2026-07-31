# Ticket 05 - Source Strategy

Status: closed
Type: grilling
Labels: `wayfinder:grilling`
Claim: claimed by Claude (wayfinder session, 2026-07-31)
Blocks: Ticket 06 - Evaluation Harness, Ticket 08 - OpenSpec Bridge

## Question

Which sources should the MVP trust, compare, exclude, or keep as disabled candidates?

## Why This Matters

The current active set is Kommersant, Interfax, Lenta, Vedomosti, and RIA. RBC is disabled because of Qrator blocking; TASS returned 403 from the server. Source mix directly shapes the radar's view of agenda pressure.

## Research Scope

Investigate:

- whether RBC has a stable RSS/API/archive route that avoids brittle scraping;
- whether TASS has a legitimate accessible route;
- whether additional sources should cover business, state media, opposition/independent media, regional signals, or international wires;
- what each source contributes and what bias/noise it adds;
- whether source categories should become part of scoring.

## Decision Shape

Produce a source policy:

- active sources for MVP;
- disabled candidates with reasons;
- source categories;
- available text depth by source: headline only, headline plus description/lead, or full article candidate;
- minimum source diversity for each signal type;
- future source backlog.

## Forward Note (2026-07-31, captured while grilling Ticket 02 - Signal Ontology, not resolved)

Airat wants the news-ingestion layer treated as a pluggable module behind a stable interface — today it's a handful of Russian news-site parsers, but the interface should be able to add RSS feeds, Telegram channels, or third-party datasets (e.g. Kaggle) without changing downstream analysis. He mentioned we may already have a ~6-year US news-background dataset somewhere and offered to locate it precisely later. This connects directly to the map's existing "Not yet specified" item — "whether to keep Russian-only scope or add foreign news sources for geopolitical context" — resolve both together when this ticket is claimed rather than deciding the interface shape without also deciding scope.

Second forward note (2026-07-31, captured while grilling One-source anomaly on Ticket 02): Airat wants source categorization — state-aligned (РБК/Коммерсант/Ведомости), independent/opposition (e.g. Радио Свобода), foreign (tagged by country) — to feed the One-source anomaly vs Noise/routine split. His reasoning: a story reported only by an independent/opposition outlet, with state-aligned outlets silent, may reflect selective non-coverage rather than the story being unreal or unimportant — source count alone shouldn't downgrade it. This is exactly the "state media, opposition/independent media... what bias/noise it adds... whether source categories should become part of scoring" work already in this ticket's Research Scope — Ticket 02 only records the principle (source category is a modifier on top of the follow-up/market-corroboration check, not a replacement for it); the actual category definitions and scoring weight belong here.

## Forward Note (2026-07-31, dataset located)

The ~6-year US news-background dataset mentioned above is `db/hf_news.db` (SQLite, READ-ONLY per `CLAUDE.md`, not present in this checkout). Corrected facts, verified against the loader code rather than assumed:

- It is overwhelmingly a single English-language financial-news corpus (`Brianferrell787/financial-news-multisource` via `src/parsers/hf_news_loader.py`), filtered to `date >= 2021-01-01`, ~2.52M articles, ~9.93 GB (`analytics/SESSION_LOG.md`). Actual content stops around 2025-09-06 (`GAP_START` in `src/parsers/gdelt_loader.py`), so the usable full-text window is ~4.5 years (2021-01 to 2025-09), not 6.
- It is **not** a matched multi-year Russian corpus. The only Russian content is `ru_archive:`-prefixed rows from `ru_news_archive_loader.py`, a small backfill (order ~28k articles per `src/agent/news_retriever.py`) covering only the post-gap tail (~2025-09 onward), from the same 5 RU sites the live collector already uses.
- Coverage past 2025-09-06 is filled separately by `db/news_gdelt.db` (English, pre-aggregated daily topic counts only, no article text); there is no equivalent pre-aggregated post-gap source for Russian beyond the live collector itself.
- `db/hf_news.db` is also the source for a separate one-off study, [Ticket 12 - Historical Cluster Calibration Study](12-historical-cluster-calibration.md) — that ticket owns deriving cluster-size/persistence/source-spread numbers from it. This ticket's own scope (source policy, categories, text-depth-by-source) is unaffected and does not need to reopen just because the dataset is now located.

## Research (2026-07-31)

Primary-source research into RSS/API access routes, legal designations, and international wires was run before grilling — see [`research/05-source-strategy-findings.md`](../research/05-source-strategy-findings.md) for the full citation trail (each claim tagged Confirmed / Reported-not-verified / Uncertain). Key facts that shaped the Working Decision below:

- `rssexport.rbc.ru/rbcnews/news/30/full.rss` is a live, current RSS feed on a different subdomain than the Qrator-blocked `www.rbc.ru` front end; RBC's terms are silent on RSS specifically (unlike Interfax/TASS, which explicitly forbid it).
- `tass.ru/rss/v2.xml` returns 403 (matches this ticket's existing finding); `tass.com`'s own Terms of Use (Section 4.5) explicitly forbid RSS-feed use without a written agreement.
- gazeta.ru, iz.ru, forbes.ru, banki.ru, and the Moscow-based novayagazeta.ru all have confirmed-live public RSS feeds with no legal designation on the outlet itself (novayagazeta.ru's media license was revoked in 2022 but it isn't itself listed as foreign-agent/undesirable).
- meduza.io is a designated "undesirable organization" (2023) plus a separate Roskomnadzor network block (2022); its RSS is confirmed live from outside Russia.
- svoboda.org (Radio Svoboda / RFE-RL) carries the same double barrier (undesirable-org 2024 + RKN block 2022); its RSS listing page returned 403 even from outside Russia — feed URL unconfirmed.
- agents.media (Agentstvo) has a confirmed-live RSS feed; foreign-agent designated (lighter tier than undesirable-org).
- Verstka, iStories, The Bell, and Novaya Gazeta Europe (distinct from the Moscow novayagazeta.ru) all have unconfirmed RSS URLs in this research; The Bell's foreign-agent status itself is genuinely unresolved, not just unconfirmed.
- Of Reuters/AP/Bloomberg, only Bloomberg has a confirmed-live free public RSS feed; it is English-only and markets-focused. All fetches were made from outside Russia — reachability from the collector's actual server is a separate, unverified question for every "confirmed live" result above.

## Working Decision

**Active sources for the MVP (14).** All write into the same `headline_snapshots` shape regardless of collection mechanism.

| Source | Mechanism | Route | Category | Text depth |
|---|---|---|---|---|
| kommersant, interfax, lenta, vedomosti, ria | `html_archive` (existing) | direct | state-aligned | headline-only |
| rbc | `rss` (`rssexport.rbc.ru`) | direct — **pending verification from the collector's production host** | state-aligned | headline+lead |
| gazeta.ru, iz.ru | `rss` | direct | state-aligned | headline+lead |
| forbes.ru, banki.ru | `rss` | direct | business/independent | headline+lead |
| novayagazeta.ru (Moscow) | `rss` | direct | independent-opposition | headline+lead |
| meduza.io | `rss` | **vpn** | independent-opposition + foreign (Latvia); tags: undesirable organization, RKN-blocked | headline+lead |
| svoboda.org | `rss` | **vpn** | independent-opposition + foreign (US, RFE/RL); tags: undesirable organization, RKN-blocked | headline+lead — **conditionally active, feed URL needs confirmation before wiring into the regimen** |
| agents.media | `rss` | direct | independent-opposition; tag: foreign agent | headline+lead |

**Disabled: tass.ru.** Reason: `tass.com`'s Terms of Use (Section 4.5) explicitly forbid RSS/feed ingestion without a written agreement, and `tass.ru` itself returns 403 directly. This is treated differently from RBC (whose terms are silent on RSS) — an explicit contractual prohibition is respected outright, not weighed against the project's private/non-redistributed use.

**Backlog (technically or scope-blocked, not policy-blocked):**
- fontanka.ru, Verstka, iStories, Novaya Gazeta Europe — RSS feed URL not confirmed in research; needs a manual lookup on the live site before these can move to active.
- The Bell — foreign-agent status itself is unresolved (conflicting/thin evidence); needs a primary-registry check before it's placed in any category, not just a feed-URL check.
- Bloomberg, Reuters, AP — English-language wire services. Parked until/unless a dedicated English-language pipeline is justified; out of scope while the project stays Russian-only (see below). Only Bloomberg currently has a confirmed free public RSS feed; Reuters and AP have no working official public RSS at all today.

**Legal-designation policy: label, not gate.** Foreign-agent and undesirable-organization designations, and Roskomnadzor network blocks, are three independent axes, tracked as descriptive facts (by source and by country of registration), not inclusion gates. Rationale: the project is currently solo/Airat-only, retrospective-only, and does not publish or redistribute content (per [Ticket 01](01-product-contract.md); Telegram delivery stays disabled until [Ticket 07 - Delivery Policy](07-delivery-policy.md) is resolved) — it performs technical/mathematical analysis (tone, word frequency, clustering) on collected headlines and stores the result privately, without editorializing about the source's legitimacy. **This must be revisited when Ticket 07 closes**, since publishing or redistributing content from a designated source is a materially different legal posture than private analysis.

**Roskomnadzor network blocks are a pure technical gate, solved by routing, not by exclusion.** Where a source is blocked at the network level from Russia (meduza.io, svoboda.org), the project's existing VPN capability (maintained elsewhere in the project, outside this checkout) is used for that specific source only, via a per-source `route: direct | vpn` config field — not a global VPN toggle for the whole collector.

**Explicit contractual prohibitions are respected outright.** TASS's written ban on RSS/feed use is not treated as a gray area the way RBC's silence is — "явно запрещено — мы с этим не спорим."

**Source categorization.** Three axes recorded per source: (1) alignment — `state-aligned` / `independent-opposition` / `business` (a fourth, distinct from either); (2) `foreign` tag with country, where the outlet is foreign-registered (meduza.io: Latvia; svoboda.org: US/RFE-RL); (3) legal-designation tags (foreign-agent / undesirable-organization / RKN-blocked) as plain facts, per the label-not-gate policy above. Purpose, per Airat: independent/opposition sources exist to **balance** the overall pressure signal against state-aligned sources, not because they're more or less trustworthy — this is the mechanism Ticket 02's "One-source anomaly vs. Noise/routine" forward note asked this ticket to define.

**Text depth by source.** Headline-only remains the default (matches the existing architecture principle of not depending on full article text early). Where a source's RSS feed already includes a description/lead field for free (all the RSS-based sources above), that field is captured as `headline+lead` — no extra scraping trip is added to get it. Full article text is out of scope for the live regimen; it stays behind the existing, separate `ru_news_archive_loader.py` backfill path into `db/hf_news.db` (read-only, per `CLAUDE.md`), not the daily/weekly/monthly/history regimen.

**Minimum source diversity per signal type — numeric thresholds intentionally deferred.** The existing `source_spread` cross-source-synchronization thresholds (3 sources for daily/weekly, 4 for monthly/history) were calibrated against the old 5-source set; with 14 active sources these become a materially weaker bar, but no new number is picked by guesswork here. This is explicitly parked for an **expanded scope on [Ticket 12 - Historical Cluster Calibration Study](12-historical-cluster-calibration.md)**: the calibration study now needs to cover the new 14-source Russian set (not only `hf_news.db`'s English-heavy archive), over as deep a historical window as can practically be backfilled (target: full year). Any resulting threshold — source-count-based, or a future numeric `category_spread` version — is stored in the versioned parameter registry from [Ticket 02](02-signal-ontology.md) as an explicitly empirically-derived, updatable value, not a hardcoded guess.

**`category_spread` — a new descriptive (non-numeric) report field, added now.** Independent of the numeric-threshold deferral above, every cluster in the report gets a plain-text tag — `state-only` / `independent-only` / `mixed` — based on which active sources covered it. No threshold, no gating; it is purely informational, so a reader can see at a glance whether a "synchronized story" reflects genuine cross-spectrum agreement or only state-aligned unanimity.

**Collection cadence (mechanical, not product-facing).** RSS-based sources are polled hourly, not once daily, purely to avoid losing items when a source's feed rolls over faster than the collection interval (RBC's feed alone carries 20+ items at any snapshot). Deduplication is by URL (existing `article_dedup` mechanism) plus the existing fuzzy-title-dedup principle, which also absorbs near-duplicate headlines generated by breaking-news spikes across polls. HTML-archive sources are unchanged — they are already date-indexed and have no gap risk. This is distinct from, and does not resolve, the separate **product-level** cadence question ("top-10 headlines/hour" as a market-coupling signal) already parked on [Ticket 10 - Market Coupling Model](10-market-coupling-model.md).

**Scope: Russian-language only for now.** International wire services are explicitly out of scope until a dedicated English-language pipeline (separate tokenization/clustering path — the current NLP layer is tuned for Russian) is justified. This resolves the map's "Not Yet Specified" item on Russian-only vs. foreign-source scope, in favor of Russian-only for the MVP.

**Pluggable ingestion contract (shape only, not implementation).** Each source in `config/news_pressure_sources.yaml` gets two new fields: `kind: html_archive | rss | telegram | dataset` and `route: direct | vpn`. Every `kind` handler must produce the same output shape — `(url, title, published_date, source, lead_text?)` — so the analysis layer never needs to know which mechanism produced a row. `telegram` and `dataset` (e.g. Kaggle) are reserved in the contract now but have no implementation this session; the working prototype scope stays "whatever is in open, direct, easily reachable access today" (site scraping or RSS), per Airat's framing.

**Explicitly out of scope for this ticket, carried forward:** the ~12-month historical backfill Airat asked about mid-session is a separate implementation task layered on top of these source decisions (and overlaps with Ticket 12's now-expanded scope) — this ticket decides *which* sources and *what depth*, not *how far back* to backfill them.

## Grilling Transcript

**Q1.** Should the legal-designation status (foreign-agent / undesirable-organization) of a candidate source block its inclusion in the MVP, or should it be a scoring/category label only, given the project is currently solo/private and doesn't publish or redistribute content?
Recommendation offered: label, not gate, for now — revisit when Ticket 07 (Delivery Policy) closes, since publishing changes the legal calculus.
Airat's answer: confirmed the private/technical framing explicitly — "Мы берем то что есть в сети, обязательно маркируем... мы это не будем куда-то интерпретировать или что-то говорить об этом... мы просто технически это забираем, маркируем в зависимости от страны происхождения источника, говорим, что это Роскомнадзору разрешает/не разрешает... мы это не распространяем, мы на основе этого делаем технически математический анализ тональности и слов и просто записываем это у себя... пока это для нас, для приватного использования, мы за это деньги не берём, никому это не рассказываем, но просто анализируем сложившуюся информацию."

**Q2.** Should RBC be reactivated via the `rssexport.rbc.ru` RSS route, which technically bypasses the Qrator wall on `www.rbc.ru`?
Recommendation offered: yes, mark it as needing a production-host reachability check before wiring into the regimen.
Airat's answer: agreed, and noted the project already has a VPN service (maintained in another checkout/branch, `c:\project\signal_mind\vpn\`, not present in this working directory) specifically built to bypass blocks on foreign sources — flagged for reference, not verified in this session. "Да в целом согласен... rbc.ru раздел новости, быстрые, чёткие, резкие, они нам очень помогут."

**Q3.** Should TASS be dropped from consideration, given `tass.com`'s Terms of Use explicitly forbid RSS ingestion without a written agreement (unlike RBC, where the terms are simply silent)?
Recommendation offered: yes, drop it — an explicit contractual prohibition is a different case from RBC's silence, even under the private-use framing from Q1.
Airat's answer: agreed without qualification — "То, что явно юридически запрещено, то запрещено. Мы с этим не спорим."

**Q4.** Should the five Common-Crawl-candidate domains with clean legal status and confirmed-live RSS (gazeta.ru, iz.ru, forbes.ru, banki.ru, novayagazeta.ru) all be added to the active set now?
Recommendation offered: yes, all five — technically ready today, no legal risk beyond what Q1 already accepted, and novayagazeta.ru directly fills the independent-opposition category gap.
Airat's answer: "да, конечно, согласен."

**Q5.** Should Meduza be added to the active set, using VPN routing to solve its Roskomnadzor network block, with its "undesirable organization" status recorded as a label per Q1?
Recommendation offered: yes.
Airat's answer: confirmed, and added the reasoning behind wanting independent/opposition sources at all: "Медузу грузим, да, это независимый источник... они будут балансировать, создавать баланс нашему общему фону, думаю."

**Q6.** Should Radio Svoboda be added on the same basis as Meduza, despite its RSS listing page returning 403 even from outside Russia (unconfirmed feed URL, distinct from Meduza where the feed itself was confirmed live)?
Recommendation offered: yes, same policy, but mark it conditionally active pending a working feed URL, since (unlike RBC) the feed itself, not just the reachability, is unconfirmed.
Airat's answer: confirmed together with Meduza in the same answer — "Радио Свобода тоже включаем, тоже независимый источник, оба оппозиционные, но они будут балансировать."

**Q7.** Of the five backlog independents found in research (Verstka, iStories, The Bell, Agentstvo, Novaya Gazeta Europe), should only Agentstvo (the one with a confirmed-live RSS feed) be added now, with the rest parked in backlog pending manual feed-URL/status verification?
Recommendation offered: yes — don't spend grilling time on unconfirmed technical details for the other four.
Airat's answer: "Да, пока так, не надо всех прям добавлять. Если будем терять, не добирать объём новостей, тогда будем придумывать что-то другое. Хорошо? Пока в бэклог."

## Forward Note (2026-07-31, captured while grilling Ticket 06 - Evaluation Harness, not resolved)

Surfaced while discussing Ticket 06's эталон calibration anchors (known economic-calendar events — CB rate decisions, Rosstat releases, quarterly earnings/audit reports, MOEX corporate events — used as "known-answer test" checkpoints for the statistical method). Airat: "нам надо кстати ещё добавить это точно источник биржу московскую, потому что там тоже нужно... она тоже часто очень экономические новости даёт очень конкретного точного толку, и к ним тоже скорее всего привязана какое-либо событие... но точно нужно добавить... 100% давайте добавим."

**Proposed addition, not yet formally decided as a 15th active source:** MOEX (Moscow Exchange) itself as a **news** source — its own corporate/economic announcements and press releases — distinct from its existing role as the market-data API (Ticket 10, `iss.moex.com`). Needs the same feasibility check the other 14 sources went through before this ticket closed (RSS/API route, terms-of-use check, reachability) — not yet verified. Whoever next touches source strategy should treat this as a concrete candidate to formally add, not just backlog speculation.

**Q8.** Should international wire services (Bloomberg being the only one with a confirmed-live free public RSS) be added now for geopolitical context, or should the project stay Russian-only?
Recommendation offered: stay Russian-only — the NLP/clustering layer is tuned for Russian, and Bloomberg specifically is markets-focused English content, not a general geopolitical wire; park English sources in backlog as a future separate pipeline.
Airat's answer: "Да, давай пока только российские данные, только русский язык, лучше пока для начала использовать это."

**Q9.** Reviewed a draft category table (state-aligned / independent-opposition / business / foreign-tagged) across all 14 active sources — any corrections?
Airat's answer: "Да, давай, хорошая раскладка." No corrections requested; also flagged that future added sources should come with their own categorization and coverage description at addition time, not deferred.

**Q10.** Should text depth stay headline-only as the default, capturing `headline+lead` only where an RSS feed already includes it for free, with full-article text staying behind the separate archive-loader path rather than the live regimen?
Recommendation offered: yes, this division.
Airat's answer: agreed — "давай хорошо, для проекта, для начала этого хватит, как заголовок" — then raised a separate question about collection *frequency/volume* (addressed in Q11), and a separate desire for ~12 months of historical depth (recorded as an explicitly out-of-scope carry-forward in the Working Decision, not resolved in this ticket).

**Q11.** Should RSS-based sources be polled hourly (rather than once daily) purely to avoid losing items to feed rollover, with the existing URL-based and fuzzy-title dedup handling repeats — while explicitly parking the separate *product-level* cadence question ("top-10 headlines/hour" as a market-coupling signal) for Ticket 10?
Recommendation offered: yes, hourly polling as a purely technical/completeness measure, distinct from the analytical cadence question already parked on Ticket 10.
Airat's answer: "Да, тут согласен. Фиксировать, с чего забрали, дубликаты убирать, чтобы не было ложных перелимитов по новостям — какое-то событие если резко произойдёт, то оно везде будет частью попадать в заголовок."

**Q12.** Should the `source_spread` numeric thresholds (currently 3/4, calibrated against the old 5-source set) be left unchanged for now, deferring any recalibration to an expanded Ticket 12 covering the new 14-source set, rather than guessing a new number today? And separately, should a non-numeric `category_spread` label (`state-only` / `independent-only` / `mixed`) be added to the report now, since it needs no calibration?
Recommendation offered: yes to both — don't guess a number Ticket 12 will measure anyway; do add the label, since it's descriptive, not a threshold.
Airat's answer: on the numeric deferral: "Мы примем решение тогда, когда мы... соберём новую базу на 11 источниках [now 14]... глубже, до года... на основе данных, в день по источникам, эмпирически, какими-то тестами, итерациями и экспертизой аналитикой мы придём к этому значению... заведём это значение в словарь [the parameter registry]... но на основе данных, которые у нас есть, не просто так из головы." On the category_spread label specifically, after a clarifying re-ask distinguishing it from the numeric question: "да."

**Q13.** Should the pluggable-ingestion contract be recorded now only as a shape (`kind`/`route` fields per source, uniform output regardless of mechanism), with Telegram and third-party-dataset ingestion reserved but not implemented this session?
Recommendation offered: yes — reserve the shape, don't design Telegram/dataset logic until it's actually needed.
Airat's answer: "Да. Сейчас просто парсим с новостного сайта или RSS, если он доступен. Расширение данных из Telegram, из БД будем закладывать на будущее. Сейчас для прототипа мы берём, по сути, что лежит в открытом прямом доступе и легко достать."

Final synthesis of the full Working Decision was read back to Airat in one pass before closing; confirmed: "да, давай, мы всё обсудили. Всё верно. Все зоны закрыли в этом тикете."
