"""Phase C — RAG Extractor.

For each (company, year) pair: query ChromaDB, feed chunks to DeepSeek,
extract structured RagHypothesis objects, save to analytics/phase_c/hypotheses/.

Usage:
    python -m src.pipeline_c.rag_extractor
    python -m src.pipeline_c.rag_extractor --company sberbank --year 2023
    python -m src.pipeline_c.rag_extractor --dry-run   # print hypotheses, no save
"""
from __future__ import annotations

import argparse
import io
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                  line_buffering=True)

from src.agent import rag as rag_module
from src.agent.llm import chat
from src.pipeline_c.hypothesis_schema import (
    RagHypothesis, VALID_INSTRUMENTS, VALID_TOPICS,
)

OUT_DIR = ROOT / "analytics" / "phase_c" / "hypotheses"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ──────────────────────────────────────────────────────────────────────────────
# Companies and years available in ChromaDB
# ──────────────────────────────────────────────────────────────────────────────

COMPANY_INSTRUMENT_MAP = {
    "sberbank":  ["MOEXFN", "IMOEX"],
    "lukoil":    ["MOEXOG", "BRENT"],
    "gazprom":   ["MOEXOG", "BRENT"],
    "yandex":    ["MOEXIT", "IMOEX"],
    "nornikel":  ["MOEXMM", "GOLD"],
}

YEARS = [2020, 2021, 2022, 2023, 2024]

# Queries that surface predictive statements in annual reports
EXTRACTION_QUERIES = [
    "чувствительность к процентной ставке влияние на прибыль",
    "риски санкций влияние на выручку экспорт",
    "валютные риски USD RUB влияние на финансовые результаты",
    "инфляция влияние на операционные расходы",
    "цены на нефть газ влияние на выручку",
    "прогноз менеджмента на следующий год",
    "макроэкономические факторы влияющие на бизнес",
]

# ──────────────────────────────────────────────────────────────────────────────
# LLM prompt
# ──────────────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """Ты — финансовый аналитик, извлекающий проверяемые гипотезы о рыночных сигналах из корпоративных отчётов.

Твоя задача: из предоставленных фрагментов отчёта извлечь КОНКРЕТНЫЕ, ИЗМЕРИМЫЕ утверждения о том, как макроэкономические факторы влияют на рыночную цену компании или её финансовые показатели.

Правила:
1. Только конкретные причинно-следственные связи с временным горизонтом
2. Каждая гипотеза должна быть проверяема на исторических данных
3. instrument — тикер на MOEX или форекс-пара (только из допустимого списка)
4. topic — один из: oil, rate, ruble, sanctions, inflation, banking, gold
5. lag_days — реалистичный временной лаг в днях [1, 7, 14, 30, 60, 90]
6. direction — "positive" (рост темы → рост инструмента) или "negative"
7. Не придумывай — только то, что явно следует из текста

Верни JSON-массив. Каждый элемент:
{
  "instrument": "MOEXFN",
  "topic": "rate",
  "lag_days": [7, 14, 30],
  "direction": "negative",
  "rationale": "Ставка +1% → давление на NIM банков → снижение прибыли → падение акций финсектора",
  "source_section": "Факторы риска",
  "confidence": 0.8
}

Допустимые инструменты:
  Международные: USD_RUB, EUR_RUB, BRENT, GOLD, SILVER, SP500, MSCI_WORLD, DXY, MSCI_INDIA, CHINA_H_SHARES, FTSE_CHINA_50, DJ_SOUTH_AFRICA
  MOEX: IMOEX (широкий рынок), MOEXFN (финансы), MOEXOG (нефть и газ), MOEX10 (голубые фишки), RUGOLD (золото MOEX)
Допустимые темы: oil, rate, ruble, sanctions, inflation, banking, gold

Если в тексте нет проверяемых гипотез — верни пустой массив [].
"""


def _build_user_prompt(
    company: str,
    year: int,
    chunks: list[dict],
    primary_instruments: list[str],
) -> str:
    chunk_text = "\n\n".join(
        f"[{c['meta'].get('company','?').capitalize()} {c['meta'].get('year','?')}, "
        f"p.{c['meta'].get('page','?')} | {c['meta'].get('section', '')}]\n"
        f"{c['text'][:600].strip()}"
        for c in chunks
    )
    return (
        f"Компания: {company.capitalize()}  Год отчёта: {year}\n"
        f"Основные инструменты: {', '.join(primary_instruments)}\n\n"
        f"=== Фрагменты отчёта ===\n\n{chunk_text}\n\n"
        f"Извлеки проверяемые рыночные гипотезы из этих фрагментов."
    )


# ──────────────────────────────────────────────────────────────────────────────
# JSON parser — robust to LLM wrapping in markdown code blocks
# ──────────────────────────────────────────────────────────────────────────────

def _parse_json_array(text: str) -> list[dict]:
    text = text.strip()
    # Strip markdown fences
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(
            line for line in lines
            if not line.startswith("```")
        ).strip()
    # Find first [ and last ]
    start = text.find("[")
    end   = text.rfind("]")
    if start == -1 or end == -1:
        return []
    try:
        return json.loads(text[start:end + 1])
    except json.JSONDecodeError:
        return []


# ──────────────────────────────────────────────────────────────────────────────
# Core extraction
# ──────────────────────────────────────────────────────────────────────────────

def extract_hypotheses(
    company: str,
    year: int,
    dry_run: bool = False,
    verbose: bool = True,
) -> list[RagHypothesis]:
    """Extract hypotheses from one (company, year) pair.

    Queries ChromaDB with multiple queries to maximise coverage,
    deduplicates chunks, asks DeepSeek to extract hypotheses,
    validates and returns RagHypothesis objects.
    """
    primary_instruments = COMPANY_INSTRUMENT_MAP.get(company, ["IMOEX"])

    def log(msg: str) -> None:
        if verbose:
            print(f"  [{company}/{year}] {msg}")

    log(f"Querying ChromaDB ...")
    all_chunks: list[dict] = []
    seen_pages: set = set()

    for query in EXTRACTION_QUERIES:
        chunks = rag_module.search_corp(query, company=company, year=year, top_k=4)
        for c in chunks:
            page_key = (c["meta"].get("filename", ""), c["meta"].get("page", 0))
            if page_key not in seen_pages:
                seen_pages.add(page_key)
                all_chunks.append(c)

    log(f"{len(all_chunks)} unique chunks retrieved")
    if not all_chunks:
        log("No chunks — skipping")
        return []

    # Sort by relevance (lower distance = more relevant)
    all_chunks.sort(key=lambda x: x["distance"])
    # Cap at 20 chunks to stay within LLM context
    top_chunks = all_chunks[:20]

    # Also pull regulatory context for this year
    reg_chunks = rag_module.search_regulatory(
        f"ключевая ставка инфляция {year}", year=year, top_k=3
    )
    for c in reg_chunks:
        top_chunks.append(c)

    log(f"Sending {len(top_chunks)} chunks to DeepSeek ...")
    t0 = time.time()
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": _build_user_prompt(
            company, year, top_chunks, primary_instruments
        )},
    ]
    try:
        response = chat(messages, temperature=0.2)
    except Exception as e:
        log(f"LLM error: {e}")
        return []
    elapsed = time.time() - t0
    log(f"LLM response in {elapsed:.1f}s ({len(response)} chars)")

    raw_items = _parse_json_array(response)
    log(f"Parsed {len(raw_items)} raw items from LLM")

    hypotheses: list[RagHypothesis] = []
    for item in raw_items:
        try:
            hyp = RagHypothesis(
                instrument     = item.get("instrument", ""),
                topic          = item.get("topic", ""),
                lag_days       = item.get("lag_days", [7, 14, 30]),
                direction      = item.get("direction", "unknown"),
                rationale      = item.get("rationale", ""),
                source_company = company,
                source_year    = year,
                source_page    = 0,
                source_section = item.get("source_section", ""),
                confidence     = float(item.get("confidence", 0.5)),
                feature_type   = "keyword_z90",
            )
            ok, reason = hyp.is_valid()
            if ok:
                hypotheses.append(hyp)
                log(f"  OK  {hyp.instrument} ~ {hyp.topic} lag={hyp.lag_days} dir={hyp.direction}")
            else:
                log(f"  SKIP  invalid: {reason} — {item}")
        except Exception as e:
            log(f"  SKIP  parse error: {e} — {item}")

    log(f"{len(hypotheses)} valid hypotheses extracted")

    # Also try embedding_z90 variants for high-confidence items
    embedding_variants = []
    for hyp in hypotheses:
        if hyp.confidence >= 0.7:
            v = RagHypothesis(
                instrument     = hyp.instrument,
                topic          = hyp.topic,
                lag_days       = hyp.lag_days,
                direction      = hyp.direction,
                rationale      = hyp.rationale,
                source_company = hyp.source_company,
                source_year    = hyp.source_year,
                source_page    = hyp.source_page,
                source_section = hyp.source_section,
                confidence     = hyp.confidence,
                feature_type   = "embedding_z90",
            )
            embedding_variants.append(v)
    hypotheses.extend(embedding_variants)
    if embedding_variants:
        log(f"Added {len(embedding_variants)} embedding_z90 variants")

    if not dry_run:
        _save(company, year, hypotheses)

    return hypotheses


def _save(company: str, year: int, hypotheses: list[RagHypothesis]) -> None:
    fname = OUT_DIR / f"{company}_{year}.json"
    data = [h.to_dict() for h in hypotheses]
    fname.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  Saved {len(hypotheses)} hypotheses → {fname.name}")


# ──────────────────────────────────────────────────────────────────────────────
# Load previously saved hypotheses
# ──────────────────────────────────────────────────────────────────────────────

def load_all_hypotheses() -> list[RagHypothesis]:
    """Load all saved hypotheses from analytics/phase_c/hypotheses/."""
    all_hyps: list[RagHypothesis] = []
    for fpath in sorted(OUT_DIR.glob("*.json")):
        try:
            items = json.loads(fpath.read_text(encoding="utf-8"))
            for d in items:
                try:
                    all_hyps.append(RagHypothesis.from_dict(d))
                except Exception:
                    pass
        except Exception:
            pass
    return all_hyps


def load_hypotheses(company: str, year: int) -> list[RagHypothesis]:
    fpath = OUT_DIR / f"{company}_{year}.json"
    if not fpath.exists():
        return []
    items = json.loads(fpath.read_text(encoding="utf-8"))
    return [RagHypothesis.from_dict(d) for d in items]


# ──────────────────────────────────────────────────────────────────────────────
# Obsidian vault dedup filter
# ──────────────────────────────────────────────────────────────────────────────

def filter_known_signals(hypotheses: list[RagHypothesis]) -> list[RagHypothesis]:
    """Use Obsidian vault to skip hypotheses that are already well-studied.

    A hypothesis is skipped if the vault returns a 'signal' chunk with
    status 'active' or 'under_attack' for the same (instrument, topic) pair.
    """
    filtered = []
    for hyp in hypotheses:
        query = f"{hyp.instrument} {hyp.topic} signal"
        vault_hits = rag_module.search_methodology(query, n_results=3, filter_type="signal")
        skip = False
        for hit in vault_hits:
            status = hit["meta"].get("status", "")
            file_name = hit["meta"].get("file", "").lower()
            # Only skip if BOTH instrument AND topic appear in the vault file name
            # (avoids false positives from partial matches)
            inst_match  = hyp.instrument.lower() in file_name
            topic_match = hyp.topic.lower() in file_name
            if inst_match and topic_match and status in ("active", "under_attack"):
                skip = True
                break
        if not skip:
            filtered.append(hyp)
    return filtered


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Phase C RAG Extractor")
    parser.add_argument("--company", default="", help="Single company (default: all)")
    parser.add_argument("--year",    type=int, default=0, help="Single year (default: all)")
    parser.add_argument("--dry-run", action="store_true", help="Print only, don't save")
    parser.add_argument("--filter-vault", action="store_true",
                        help="Skip hypotheses already in Obsidian vault")
    args = parser.parse_args()

    companies = [args.company] if args.company else list(COMPANY_INSTRUMENT_MAP.keys())
    years     = [args.year]    if args.year     else YEARS

    total = 0
    ts    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n=== Phase C RAG Extractor  {ts} ===")
    print(f"Companies: {companies}")
    print(f"Years:     {years}")
    print(f"Dry run:   {args.dry_run}")
    print()

    for company in companies:
        for year in years:
            hyps = extract_hypotheses(company, year, dry_run=args.dry_run, verbose=True)
            if args.filter_vault and hyps:
                before = len(hyps)
                hyps = filter_known_signals(hyps)
                print(f"  Vault filter: {before} → {len(hyps)} (skipped {before - len(hyps)} known)")
            total += len(hyps)
            print()

    print(f"=== Done. Total hypotheses extracted: {total} ===")


if __name__ == "__main__":
    main()
