"""Index Obsidian vault into ChromaDB 'methodology' collection.

Reads all .md files from obsidian/ (except 00_System/),
chunks by H2 sections, upserts into ChromaDB.

Run after updating vault:
    python -m src.parsers.obsidian_indexer

Chunk ID format: {file_stem}__{section_slug}
This is stable across re-runs → upsert never duplicates.
"""
from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import chromadb
from sentence_transformers import SentenceTransformer

VAULT_DIR   = Path(__file__).parents[2] / "obsidian"
CHROMA_DIR  = Path(__file__).parents[2] / "db" / "chroma"
COLLECTION  = "methodology"
MODEL_NAME  = "paraphrase-multilingual-MiniLM-L12-v2"

# Folders to skip (system prompts not useful for agent RAG)
SKIP_FOLDERS = {"00_System"}


def _slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    text = re.sub(r"[^\w\s-]", "", text).strip().lower()
    return re.sub(r"[\s-]+", "_", text)[:60]


def _parse_frontmatter(content: str) -> dict:
    """Extract YAML frontmatter as a flat dict of strings."""
    meta: dict = {}
    if not content.startswith("---"):
        return meta
    end = content.find("---", 3)
    if end == -1:
        return meta
    block = content[3:end]
    for line in block.splitlines():
        if ":" in line:
            k, _, v = line.partition(":")
            meta[k.strip()] = v.strip()
    return meta


def _chunk_by_h2(content: str, file_stem: str, meta: dict) -> list[dict]:
    """Split markdown into chunks by H2 headings.

    Returns list of {id, text, metadata}.
    First chunk: everything before first H2 (frontmatter + summary).
    IDs are deduplicated with a counter suffix when the same section slug appears twice.
    """
    # Strip frontmatter
    body = content
    if content.startswith("---"):
        end = content.find("---", 3)
        if end != -1:
            body = content[end + 3:].lstrip()

    raw_sections: list[tuple[str, str]] = []
    sections = re.split(r"^(#{1,2} .+)$", body, flags=re.MULTILINE)

    current_title = "summary"
    current_text = ""

    for part in sections:
        if re.match(r"^#{1,2} .+$", part):
            if current_text.strip():
                raw_sections.append((current_title, current_text))
            current_title = re.sub(r"^#+\s*", "", part).strip()
            current_text = part + "\n"
        else:
            current_text += part

    if current_text.strip():
        raw_sections.append((current_title, current_text))

    # Build chunks with deduplicated IDs
    slug_counter: dict[str, int] = {}
    chunks = []
    for title, text in raw_sections:
        slug = _slugify(title)
        count = slug_counter.get(slug, 0)
        slug_counter[slug] = count + 1
        unique_id = f"{file_stem}__{slug}" if count == 0 else f"{file_stem}__{slug}_{count}"
        chunks.append({
            "id": unique_id,
            "text": text.strip(),
            "metadata": {
                "file": file_stem,
                "section": title,
                "type": meta.get("type", ""),
                "status": meta.get("status", ""),
                "severity": meta.get("severity", ""),
                "instrument_a": meta.get("instrument_a", ""),
                "instrument_b": meta.get("instrument_b", ""),
                "tags": meta.get("tags", ""),
            },
        })

    return chunks


def index_vault(vault_path: str | Path | None = None) -> dict:
    """Index Obsidian vault into ChromaDB methodology collection.

    Returns stats: {files, chunks_added, chunks_updated}.
    """
    vault = Path(vault_path) if vault_path else VAULT_DIR
    model = SentenceTransformer(MODEL_NAME, local_files_only=True)
    client = chromadb.PersistentClient(path=str(CHROMA_DIR))

    col = client.get_or_create_collection(
        name=COLLECTION,
        metadata={"hnsw:space": "cosine"},
    )

    all_chunks: list[dict] = []

    md_files = [
        f for f in vault.rglob("*.md")
        # Skip files directly in vault root (planning docs) and system folder
        if f.parent != vault
        and f.parts[len(vault.parts)] not in SKIP_FOLDERS
    ]

    for md_file in sorted(md_files):
        content = md_file.read_text(encoding="utf-8")
        meta = _parse_frontmatter(content)

        # Skip archived files
        if meta.get("status") == "archived":
            continue

        file_stem = md_file.stem
        chunks = _chunk_by_h2(content, file_stem, meta)
        all_chunks.extend(chunks)

    if not all_chunks:
        print("[obsidian_indexer] No chunks found.")
        return {"files": 0, "chunks": 0}

    # Embed all chunks
    texts = [c["text"] for c in all_chunks]
    print(f"[obsidian_indexer] Embedding {len(all_chunks)} chunks from {len(md_files)} files...")
    embeddings = model.encode(texts, show_progress_bar=True).tolist()

    # Upsert into ChromaDB (stable IDs → no duplicates on re-run)
    col.upsert(
        ids=[c["id"] for c in all_chunks],
        documents=texts,
        embeddings=embeddings,
        metadatas=[c["metadata"] for c in all_chunks],
    )

    stats = {
        "files": len(md_files),
        "chunks": len(all_chunks),
        "collection_total": col.count(),
    }
    print(
        f"[obsidian_indexer] Done. Files: {stats['files']}, "
        f"Chunks upserted: {stats['chunks']}, "
        f"Collection total: {stats['collection_total']}"
    )
    return stats


if __name__ == "__main__":
    index_vault()
