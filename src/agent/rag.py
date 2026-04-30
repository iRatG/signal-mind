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

# Labels shown in LLM context block
_SOURCE_LABEL = {
    REGULATORY: "CBR",
    CORP:       "Corp",
}


@lru_cache(maxsize=1)
def _model() -> SentenceTransformer:
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


def collection_stats() -> dict:
    stats = {}
    for name in [REGULATORY, CORP]:
        try:
            col = _client().get_collection(name)
            stats[name] = col.count()
        except Exception:
            stats[name] = 0
    return stats
