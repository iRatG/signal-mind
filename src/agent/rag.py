"""RAG interface for Signal Mind agent.

Two ChromaDB collections:
  regulatory_docs  — CBR reports (KGO, MFI, banking stats)
  corp_reports     — company annual reports (Gazprom, Lukoil, Sber, etc.)

Main entry point for the agent:
  get_context(query, year=None, top_k=8) -> str
    Returns a formatted text block ready for LLM prompt injection.
"""
from __future__ import annotations
from functools import lru_cache
from pathlib import Path

import chromadb
from sentence_transformers import SentenceTransformer

CHROMA_DIR  = Path(__file__).parents[2] / "db" / "chroma"
MODEL_NAME  = "paraphrase-multilingual-MiniLM-L12-v2"

REGULATORY  = "regulatory_docs"
CORP        = "corp_reports"
METHODOLOGY = "methodology"

# Labels shown in LLM context block
_SOURCE_LABEL = {
    REGULATORY: "CBR",
    CORP:       "Corp",
    METHODOLOGY: "Vault",
}


@lru_cache(maxsize=1)
def _model() -> SentenceTransformer:
    try:
        return SentenceTransformer(MODEL_NAME, local_files_only=True)
    except Exception:
        return SentenceTransformer(MODEL_NAME)


@lru_cache(maxsize=1)
def _client() -> chromadb.PersistentClient:
    return chromadb.PersistentClient(path=str(CHROMA_DIR))


def _year_filter(year: int | None) -> dict | None:
    if year is None:
        return None
    # Include the target year and one year before/after for context
    return {"year": {"$gte": year - 1, "$lte": year + 1}}


def _query_collection(
    collection_name: str,
    query_embedding: list[float],
    where: dict | None,
    top_k: int,
) -> list[dict]:
    """Query one collection. Returns list of {text, meta, distance}."""
    try:
        col = _client().get_collection(collection_name)
    except Exception:
        return []  # collection not loaded yet

    count = col.count()
    if count == 0:
        return []

    n = min(top_k, count)
    kwargs: dict = {"query_embeddings": [query_embedding], "n_results": n,
                    "include": ["documents", "metadatas", "distances"]}
    if where:
        kwargs["where"] = where

    try:
        res = col.query(**kwargs)
    except Exception:
        # Retry without year filter if no results match
        kwargs.pop("where", None)
        try:
            res = col.query(**kwargs)
        except Exception:
            return []

    results = []
    for doc, meta, dist in zip(
        res["documents"][0], res["metadatas"][0], res["distances"][0]
    ):
        results.append({"text": doc, "meta": meta, "distance": dist,
                         "collection": collection_name})
    return results


def _format_chunk(item: dict) -> str:
    meta   = item["meta"]
    source = _SOURCE_LABEL.get(item["collection"], item["collection"])
    year   = meta.get("year", "")
    page   = meta.get("page", "")

    if item["collection"] == CORP:
        company = meta.get("company", "").capitalize()
        label = f"[{company} {year}, p.{page}]"
    else:
        doc_type = meta.get("type", "doc")
        label = f"[CBR {doc_type} {year}, p.{page}]"

    return f"{label}\n{item['text'].strip()}"


def _deduplicate(results: list[dict]) -> list[dict]:
    """Remove chunks from the same page/company to maximise diversity."""
    seen, unique = set(), []
    for item in results:
        key = (item["collection"],
               item["meta"].get("company", ""),
               item["meta"].get("filename", ""),
               item["meta"].get("page", 0))
        if key not in seen:
            seen.add(key)
            unique.append(item)
    return unique


# ── Public API ────────────────────────────────────────────────────────────────

def search_regulatory(query: str, year: int | None = None, top_k: int = 5) -> list[dict]:
    emb = _model().encode([query])[0].tolist()
    return _query_collection(REGULATORY, emb, _year_filter(year), top_k)


def search_corp(
    query: str,
    company: str | None = None,
    year: int | None = None,
    top_k: int = 5,
) -> list[dict]:
    emb = _model().encode([query])[0].tolist()
    where = _year_filter(year) or {}
    if company:
        company_filter = {"company": {"$eq": company.lower()}}
        where = {"$and": [where, company_filter]} if where else company_filter
    return _query_collection(CORP, emb, where or None, top_k)


def get_context(
    query: str,
    year: int | None = None,
    top_k: int = 8,
) -> str:
    """
    Main function for the agent.
    Queries both collections, deduplicates, sorts by relevance,
    returns a formatted text block for LLM prompt injection.

    Returns empty string if no relevant context found.
    """
    emb = _model().encode([query])[0].tolist()
    where = _year_filter(year)

    per_collection = max(top_k // 2, 3)
    raw = (
        _query_collection(REGULATORY, emb, where, per_collection) +
        _query_collection(CORP,        emb, where, per_collection)
    )

    if not raw:
        return ""

    # Sort by semantic distance (lower = more relevant)
    raw.sort(key=lambda x: x["distance"])
    unique = _deduplicate(raw)[:top_k]

    # Filter out low-relevance results (cosine distance > 0.8 = not relevant)
    unique = [r for r in unique if r["distance"] < 0.8]
    if not unique:
        return ""

    blocks = [_format_chunk(r) for r in unique]
    header = f"=== Document context ({len(blocks)} fragments) ==="
    return header + "\n\n" + "\n\n".join(blocks)


def search_methodology(
    query: str,
    n_results: int = 3,
    filter_type: str | None = None,
) -> list[dict]:
    """Search the Obsidian methodology vault via semantic similarity.

    Returns relevant chunks about signals, attacks, approaches, concepts.
    Used by the agent before hypothesis generation to retrieve:
    - known signals for the instrument pair
    - open methodological attacks
    - deprecated approaches to avoid
    - untested instruments from full scope map

    Args:
        query: natural language query, e.g. "USD/RUB MOEXFN signal attacks"
        n_results: number of top chunks to return
        filter_type: optionally filter by type field
            ('signal', 'attack', 'approach', 'concept', 'moc')
    """
    emb = _model().encode([query])[0].tolist()
    where: dict | None = None
    if filter_type:
        where = {"type": {"$eq": filter_type}}
    results = _query_collection(METHODOLOGY, emb, where, n_results)
    return results


def get_methodology_context(query: str, top_k: int = 4) -> str:
    """Return formatted methodology context for LLM prompt injection.

    Retrieves relevant vault chunks covering: known signals, open attacks,
    deprecated approaches, and untested data scope gaps.
    Returns empty string if methodology collection not yet indexed.
    """
    results = search_methodology(query, n_results=top_k)
    if not results:
        return ""

    blocks = []
    for r in results:
        meta = r["meta"]
        file_name = meta.get("file", "")
        section = meta.get("section", "")
        ftype = meta.get("type", "")
        status = meta.get("status", "")
        label = f"[Vault:{ftype} {file_name} / {section} | status:{status}]"
        blocks.append(f"{label}\n{r['text'][:400].strip()}")

    header = f"=== Methodology context ({len(blocks)} fragments) ==="
    return header + "\n\n" + "\n\n".join(blocks)


def get_context(
    query: str,
    year: int | None = None,
    top_k: int = 8,
) -> str:
    """Main function for the agent.

    Queries regulatory, corp, and methodology collections,
    deduplicates, sorts by relevance, returns formatted text for LLM prompt.
    Returns empty string if no relevant context found.
    """
    emb = _model().encode([query])[0].tolist()
    where = _year_filter(year)

    per_collection = max(top_k // 2, 3)
    raw = (
        _query_collection(REGULATORY, emb, where, per_collection) +
        _query_collection(CORP,        emb, where, per_collection)
    )

    if not raw:
        return ""

    raw.sort(key=lambda x: x["distance"])
    unique = _deduplicate(raw)[:top_k]
    unique = [r for r in unique if r["distance"] < 0.8]
    if not unique:
        return ""

    blocks = [_format_chunk(r) for r in unique]
    header = f"=== Document context ({len(blocks)} fragments) ==="
    return header + "\n\n" + "\n\n".join(blocks)


def collection_stats() -> dict:
    stats = {}
    for name in [REGULATORY, CORP, METHODOLOGY]:
        try:
            col = _client().get_collection(name)
            stats[name] = col.count()
        except Exception:
            stats[name] = 0
    return stats
