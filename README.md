# Signal Mind

A system for detecting weak market signals from financial, macroeconomic, and regulatory data using a cyclical AI agent.

## Concept

**Signal → Evidence → Opportunity → Action**

The agent continuously scans structured and unstructured data sources, generates hypotheses, verifies them against data, and accumulates patterns that indicate emerging business opportunities.

## Architecture

```
Data Layer          →  Signal Layer        →  Agent Layer
──────────────────     ──────────────────     ──────────────────
MOEX indices           Weak signal detect     Hypothesis gen
CBR rates / key rate   Evidence linking       SQL verification
Rosstat macro          Pattern storage        Ouroboros cycle
Regulatory PDFs        Signal Score 0-100     Signal Brief
Financial news
```

## Data Sources

- **MOEX** — index time series (IMOEX, MOEXFN, MOEXOG, etc.)
- **CBR** — forex rates, key rate history
- **Rosstat** — macroeconomic statistics
- **Regulatory PDFs** — CBR reports (KGO, MFI, pension funds)
- **News** — Russian financial news corpus

## Tech Stack

| Component | Technology |
|---|---|
| Numerical / time-series DB | DuckDB |
| Text / vector DB | ChromaDB |
| Embeddings | paraphrase-multilingual-MiniLM-L12-v2 |
| LLM | DeepSeek API (primary), Claude API (backup) |
| Language | Python 3.11 |

## Project Structure

```
signal_mind/
├── src/
│   ├── parsers/       # data ingestion scripts
│   ├── db/            # DuckDB + ChromaDB loaders
│   └── agent/         # hypothesis engine, ouroboros loop
├── notebooks/         # exploration and analysis
├── db/                # database files (not in git)
├── statistic/         # raw data files (not in git)
├── context/           # project documentation
└── idea/              # concept documents
```

## Phases

- **Phase 0** — Environment setup ✓
- **Phase 1** — Data loading (MOEX, CBR, Rosstat, PDFs, news)
- **Phase 2** — Data linking and analytical views
- **Phase 3** — First agent (hypothesis → SQL → evaluation)
- **Phase 4** — Ouroboros cycle (recursive verification)
- **Phase 5** — Signal Score + Signal Brief output

## Status

`Phase 1 — data ingestion in progress`
