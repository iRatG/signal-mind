"""Parse regulatory PDFs (KGO, MFI, pension) into ChromaDB."""
import hashlib
from pathlib import Path

import pdfplumber
import chromadb
from sentence_transformers import SentenceTransformer

PDF_DIR = Path(__file__).parents[2] / "statistic" / "pdf"
CHROMA_DIR = Path(__file__).parents[2] / "db" / "chroma"

MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
COLLECTION_NAME = "regulatory_docs"
CHUNK_SIZE = 900      # chars per chunk — large enough for financial sentences
CHUNK_OVERLAP = 200


PDF_META = {
    "kgo_2022.pdf":      {"type": "KGO", "year": 2022, "source": "CBR"},
    "kgo_2023.pdf":      {"type": "KGO", "year": 2023, "source": "CBR"},
    "kgo_2024.pdf":      {"type": "KGO", "year": 2024, "source": "CBR"},
    "kgo_2025.pdf":      {"type": "KGO", "year": 2025, "source": "CBR"},
    "pf_2025_Q4.pdf":    {"type": "pension_fund", "year": 2025, "source": "CBR"},
    "review_mfi_25Q4.pdf": {"type": "MFI_review", "year": 2025, "source": "CBR"},
    "Bbs2603r.pdf":      {"type": "banking_stats", "year": 2026, "source": "CBR"},
}


def chunk_text(text: str) -> list[str]:
    chunks = []
    start = 0
    while start < len(text):
        end = start + CHUNK_SIZE
        chunks.append(text[start:end].strip())
        start += CHUNK_SIZE - CHUNK_OVERLAP
    return [c for c in chunks if len(c) > 50]


def doc_id(filename: str, page: int, chunk: int) -> str:
    raw = f"{filename}_{page}_{chunk}"
    return hashlib.md5(raw.encode()).hexdigest()


def load_pdfs():
    CHROMA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Loading embedding model: {MODEL_NAME}")
    model = SentenceTransformer(MODEL_NAME)

    client = chromadb.PersistentClient(path=str(CHROMA_DIR))
    collection = client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},
    )

    total_chunks = 0

    for pdf_path in sorted(PDF_DIR.glob("*.pdf")):
        fname = pdf_path.name
        meta_base = PDF_META.get(fname, {"type": "unknown", "year": 0, "source": "CBR"})
        print(f"\n  {fname} ...")

        try:
            all_chunks, all_ids, all_metas = [], [], []

            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    text = page.extract_text() or ""
                    for chunk_idx, chunk in enumerate(chunk_text(text)):
                        cid = doc_id(fname, page_num, chunk_idx)
                        all_ids.append(cid)
                        all_chunks.append(chunk)
                        all_metas.append({
                            **meta_base,
                            "filename": fname,
                            "page": page_num,
                            "chunk": chunk_idx,
                        })

            if not all_chunks:
                print(f"    no text extracted")
                continue

            # Batch embed
            batch_size = 64
            for i in range(0, len(all_chunks), batch_size):
                batch_texts = all_chunks[i:i + batch_size]
                batch_ids = all_ids[i:i + batch_size]
                batch_metas = all_metas[i:i + batch_size]
                embeddings = model.encode(batch_texts, show_progress_bar=False).tolist()
                collection.upsert(
                    ids=batch_ids,
                    documents=batch_texts,
                    embeddings=embeddings,
                    metadatas=batch_metas,
                )

            total_chunks += len(all_chunks)
            print(f"    {len(all_chunks)} chunks from {page_num} pages")

        except Exception as e:
            print(f"    ERROR: {e}")

    print(f"\nPDF total: {total_chunks} chunks in ChromaDB")
    print(f"Collection size: {collection.count()}")


if __name__ == "__main__":
    load_pdfs()
