# Ticket 10 - Market Coupling Model

Status: open
Type: grilling
Labels: `wayfinder:grilling`
Claim: unclaimed
Blocks: Ticket 06 - Evaluation Harness, Ticket 08 - OpenSpec Bridge

## Question

What first form of relationship should News Pressure Radar measure between news pressure and market data?

## Why This Matters

Airat's clarified goal is not trading prediction. The goal is digitizing the world: people speak, events happen, news pressure forms, and market indices/currency rates leave measurable traces. The pilot needs a narrow model of this relationship before implementation.

## Working Decision

Use broad market instruments first:

- Moscow Exchange broad indices;
- Moscow Exchange sector indices;
- ruble FX pairs against USD, EUR, and CNY.

Exclude individual stocks from the pilot unless later evidence shows that sector indices are too coarse.

Airat is interested in correlation and short event windows around the news, not only after-news movement. Because public news may arrive after informed participants have already moved, the pilot should consider both pre-event and post-event windows, especially around `D-3..D+3`, with `D+5` as the outer horizon.

Cross-source news pressure is necessary but not sufficient for market coupling. The market-coupling layer should only describe a relationship when a meaningful news-pressure event also coincides with an abnormal or relative market trace in the event window.

Airat proposed a stronger first workflow: start from market anomalies rather than from news. For each index or FX pair, measure one-year baseline volatility, detect rare jumps or abnormal moves, then look back at news pressure in the previous three days and around the event. Repeat across several indices and currencies. This reduces the search space and makes the pilot more auditable.

Use a staged text-depth policy for price/quality balance. Keep headlines as the default corpus for broad scanning. Add descriptions/leads when available for candidate clusters. Fetch or analyze full article text only for a small set of market-anomaly candidates where extra context changes interpretation quality.

Data source confirmed 2026-07-31: MOEX ISS API (`iss.moex.com`) is public, no login/API key required. Verified live against `/iss/history/engines/stock/markets/index/securities/IMOEX.json` — returns real historical OHLC rows (BOARDID, TRADEDATE, CLOSE/OPEN/HIGH/LOW, VOLUME, CURRENCYID), the same shape the existing manual-ZIP loader (`src/parsers/moex_indices.py`) already parses. This can replace the manual export-and-ZIP workflow.

Candidate instrument list pulled from `/iss/engines/stock/markets/index/securities.json` and `/iss/engines/currency/markets/selt/securities.json`:
- Broad: `IMOEX` (ruble), `RTSI` (dollar-denominated, same basket)
- Sector: `MOEXOG` (oil & gas), `MOEXFN` (financials), `MOEXMM` (metals & mining), `MOEXCN` (consumer), `MOEXEU` (electric utilities), `MOEXTL` (telecom), `MOEXTN` (transport), `MOEXCH` (chemicals), `MOEXIT` (IT), `MOEXRE` (real estate)
- FX (CETS board, TOM settlement): `USD000UTSTOM`, `EUR_RUB__TOM`, `CNYRUB_TOM`

This is a sourcing fact, not the coupling-model decision below — the instrument list narrows "which market instruments are in the pilot" but the model itself (event-window vs market-first backtrace, etc.) is still open.

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
