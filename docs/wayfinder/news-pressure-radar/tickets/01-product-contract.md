# Ticket 01 - Product Contract

Status: closed
Type: grilling
Labels: `wayfinder:grilling`
Claim: claimed by Claude (wayfinder session, 2026-07-31)
Blocks: Ticket 04 - Report Prototype, Ticket 07 - Delivery Policy, Ticket 08 - OpenSpec Bridge

## Question

What is News Pressure Radar promising to Airat for the first useful MVP?

## Why This Matters

The current code can already collect, cluster, score, and write reports. The next risk is building features without a clear user contract. This ticket defines what the system must be good at before more engineering is justified.

## Decision Shape

Answer these as a single product contract:

- Primary user: Airat alone, a future audience, or a future product buyer?
- Primary moment: morning briefing, weekly review, strategic research, or publication prep?
- Primary value: "what happened", "what is accelerating", "what others miss", or "what deserves action"?
- Report confidence: when should the system speak boldly, hedge, or stay silent?
- Success after 30 days: what would make Airat say the radar is genuinely useful?

## Starting Assumption

Use the personal morning analyst as the default MVP: one compact daily report that explains agenda pressure and points to what deserves attention next.

## Working Decision

Airat chose the "understanding the world" direction, not "market hypotheses" as the primary MVP. The product should digitize how the world speaks and reacts: people make statements, news events happen, and market indices leave a measurable trace. The system should describe relationships between news pressure and market movement cautiously, as observable coupling rather than trading advice.

Resolved 2026-07-31 (`/grilling` session), answering the Decision Shape questions in full:

- **Primary user**: Airat alone. No future-audience or product-buyer design work now — that's a later decision if real demand shows up.
- **Primary moment**: daily, from day one — not phased in after a weekly-only trial. A weekly layer (e.g. a word-cloud/most-frequent-terms view across the week) can be added on top later, but daily is the non-negotiable base cadence so Airat can check in throughout the day.
- **Primary value**: "what happened" combined with "what is accelerating" — a digest with a pressure-intensity read, not a bare headline list. Explicitly NOT "what deserves action" in a directive sense.
- **Report confidence**: retrospective/historical-correlation framing only. The report states measured historical association ("this news-pressure pattern has historically coincided with elevated volatility in index X over D-3..D+3"), never a forward-looking directional expectation ("index X will likely rise/fall"). This is the guardrail that keeps the product inside the map's existing out-of-scope line against trading signals — confidence is expressed as correlation strength, not predicted direction.
- **Success after 30 days**: an objective criterion, not Airat's subjective read alone. Success means at least one statistically established, backtested reference correlation (an "эталон") between a synthesized news-tonality index and a real market/composite signal, at a defined lag (e.g. D+1), validated against historical headlines pulled from as far back as the start of the year as a calibration/backtest pool. That reference correlation becomes the benchmark other candidate signals get checked against, replacing gut-feel judgment of "is this working" with a measurable one.

**Architecture note surfaced during this session**: the product Airat described has three distinct layers, and the map already separates them into different tickets rather than needing a new one — this ticket (with Ticket 04) owns the **product/report layer**; Ticket 03 (Quality Gate Contract) owns the **technical/operational health layer** (did the pipeline's skills/collectors/daemons run cleanly); Ticket 11 (Agentic Delivery Process) owns the **intellectual/metrics-core layer** (which signal metrics are kept, ranked, or dropped, evolving per hypothesis and likely per time-window — some metrics may only hold at D+1, others at D+3). Keep these three separate when resolving downstream tickets; don't let the product report absorb the other two layers' content (see the forward notes already left on Tickets 04, 06, and 11).

## Grilling Transcript

Full record of the `/grilling` interview behind the Working Decision above — kept in addition to the synthesis because Airat asked explicitly that the actual question-and-answer exchange be preserved, not only the conclusion. Questions are as actually asked (condensed from chat, not reworded after the fact); answers keep Airat's own terms where he introduced them.

**Q1 — Primary user: для кого этот радар? (ты один / будущая аудитория / будущий покупатель продукта)**
Claude's recommendation: ты один — карта уже говорит "personal analytical loop", "no public product or dashboard yet".
Airat's answer: "пока я один". Confirmed without pushback, then immediately expanded into what mattered more to him: функциональность поиска, механизм реализации, обязательная документация, набор скиллов/навыков, и расписать всё досконально — по циклу идея → обсуждение/прожарка → спецификация → метрики → проверка → код → итерация → аудит/сверка метрик → фиксация результатов → изменение → применение изменений → следующая итерация. He framed writing this framework/skeleton as the project's main deliverable, with implementation detail delegated to `grill-me`/`wayfinder`/`openspec`/`tdd`. (This expansion was captured as a forward note on Ticket 11, not resolved here — see Ticket 11.)

**Q2 — Primary moment: когда нужен отчёт? (утренний брифинг / еженедельный обзор / стратегическое исследование / подготовка к публикации)**
Claude's recommendation: утренний брифинг как основной ритм.
Airat's answer: ежедневно, точно, с самого начала — "чтобы я в течение дня мог на это смотреть", вместе, отчёт по почте либо в Telegram. Возможно позже недельный отчёт с графиками/word cloud самых частых слов за неделю, но с самого начала — каждый день, чтобы "интегрировалось", итерационный процесс. Затем он описал желаемую структуру отчёта из 3 разделов: (1) общий — что увидел по новостям, какой индекс более коррелирован/волатилен, терминологию ещё предстоит подобрать; (2) технический — какие метрики процесса сработали/не сработали, эволюционный процесс "это работает — оставили, это не работает — убрали"; (3) общий вывод — есть ли прогресс вообще, "метрики нашей функциональности". (Captured as a forward note on Ticket 04.)

**Follow-up — раздел 2 отчёта: внутри ежедневного отчёта или отдельный лог?**
Claude предложил выбор явно. Airat: "Отдельно от продуктового отчёта" — мета-метрики процесса не смешиваются с продуктовым содержанием; кандидат-дом — `db/knowledge.md` (captured on Tickets 04 and 11).

**Q3 — Primary value: что отчёт доносит в первую очередь? ("что произошло" / "что ускоряется" / "что упускают другие" / "что заслуживает действия")**
Claude's recommendation: "что произошло" + "что ускоряется", строго ретроспективно, синтез из описания раздела 1.
Airat's answer, in his own framing: настоял на трёх РАЗНЫХ слоях продукта — (1) продуктовая составляющая, (2) внутренняя техническая ("наши скиллы, демоны, процессы отработали штатно"), (3) интеллектуальное/рациональное ядро ("какие метрики мы поставили, какие отлетели"). Описал продуктовый слой как желание видеть, что "синтезированный индекс" тональности новостей "один в один" реагирует на рынок (или наоборот — рынок реагирует первым, а новостной тон меняется вслед), с конкретным лагом (например "в 14:00" или "на следующий день"). Claude flagged this directional-expectation language against the map's existing out-of-scope line on trading signals, before finalizing Q4/Q5.

**Q4 — Report confidence: как удержать грань между "направленное ожидание" и "торговым сигналом" (уже out-of-scope)?**
Claude offered two options: (a) только ретроспективная связь — never phrase as future direction; (b) allow directional expectation language, more actionable but riskier.
Airat's answer: "Только ретроспективная связь (безопаснее)".

**Q5 — Success after 30 days: что должно произойти, чтобы радар был "реально полезен"?**
Claude's recommendation: daily habit of reading it + noticing coupling you wouldn't have spotted + evidence Section 2's audit dropped/changed at least one metric.
Airat's answer, in his own terms: настоял, что критерий должен быть объективным, не только его ощущением — "помимо моего ощущения есть фактические данные". Описал это как **"эталон"**: построить синтезированный индекс тональности новостей, который измеримо коррелирует (в ту или другую сторону, с известным лагом) с реальным рыночным/композитным сигналом, провалидированный на историческом бэктесте — "взять заголовки новостей как можно больше с самого начала года" как обучающий/калибровочный пул. Этот эталон затем становится ориентиром, с которым сверяются все прочие найденные, неочевидные сигналы. Отдельно подчеркнул, что набор метрик, вероятно, будет ансамблем, специфичным по временному окну (какая-то метрика работает на D+1, другая — на D+3), и что это должно постоянно фиксироваться/логироваться, чтобы агент-аудитор мог находить, что оставить, а что отбросить — "для нас очень важно всё фиксировать... чтобы мы на этом обучались". (Captured as a forward note on Tickets 06 and 11.)
