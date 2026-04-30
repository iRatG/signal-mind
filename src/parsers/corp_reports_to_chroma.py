"""Parse corporate annual reports (ZIP/RAR archives with PDFs) into ChromaDB.

Scans statistic/pdf/<company>/ subdirectories, extracts archives,
parses PDFs, embeds and stores in ChromaDB collection 'corp_reports'.
"""
import hashlib
import re
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import pdfplumber
import chromadb
import rarfile
from sentence_transformers import SentenceTransformer

rarfile.UNRAR_TOOL = r"C:\Program Files\WinRAR\UnRAR.exe"

BASE_DIR   = Path(__file__).parents[2]
PDF_DIR    = BASE_DIR / "statistic" / "pdf"
CHROMA_DIR = BASE_DIR / "db" / "chroma"

MODEL_NAME      = "paraphrase-multilingual-MiniLM-L12-v2"
COLLECTION_NAME = "corp_reports"
CHUNK_SIZE      = 900   # large enough for financial statements
CHUNK_OVERLAP   = 200


def extract_year(filename: str) -> int:
    m = re.search(r"\b(20\d{2})\b", filename)
    return int(m.group(1)) if m else 0


def chunk_text(text: str) -> list[str]:
    chunks, start = [], 0
    while start < len(text):
        end = start + CHUNK_SIZE
        chunks.append(text[start:end].strip())
        start += CHUNK_SIZE - CHUNK_OVERLAP
    return [c for c in chunks if len(c) > 50]


def doc_id(company: str, filename: str, page: int, chunk: int) -> str:
    raw = f"{company}_{filename}_{page}_{chunk}"
    return hashlib.md5(raw.encode()).hexdigest()


def extract_archive(archive_path: Path, dest_dir: Path) -> list[Path]:
    """Extract ZIP or RAR to dest_dir. Returns list of extracted PDF paths."""
    suffix = archive_path.suffix.lower()
    pdfs = []

    if suffix == ".zip":
        with zipfile.ZipFile(archive_path) as zf:
            for member in zf.namelist():
                if member.lower().endswith(".pdf"):
                    zf.extract(member, dest_dir)
                    # find the extracted file (may be nested)
                    for f in dest_dir.rglob("*.pdf"):
                        if f not in pdfs:
                            pdfs.append(f)

    elif suffix == ".rar":
        with rarfile.RarFile(str(archive_path)) as rf:
            for member in rf.namelist():
                if member.lower().endswith(".pdf"):
                    rf.extract(member, str(dest_dir))
            for f in dest_dir.rglob("*.pdf"):
                if f not in pdfs:
                    pdfs.append(f)

    return pdfs


def parse_pdf(pdf_path: Path, company: str, year: int) -> tuple[list, list, list]:
    all_chunks, all_ids, all_metas = [], [], []
    fname = pdf_path.name

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                text = page.extract_text() or ""
                for chunk_idx, chunk in enumerate(chunk_text(text)):
                    cid = doc_id(company, fname, page_num, chunk_idx)
                    all_ids.append(cid)
                    all_chunks.append(chunk)
                    all_metas.append({
                        "company":  company,
                        "year":     year,
                        "source":   "annual_report",
                        "filename": fname,
                        "page":     page_num,
                        "chunk":    chunk_idx,
                    })
    except Exception as e:
        print(f"      PDF error: {e}")

    return all_chunks, all_ids, all_metas


def load_corp_reports():
    CHROMA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Loading embedding model: {MODEL_NAME}")
    model = SentenceTransformer(MODEL_NAME)

    client = chromadb.PersistentClient(path=str(CHROMA_DIR))
    collection = client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},
    )

    existing = collection.count()
    print(f"ChromaDB '{COLLECTION_NAME}': {existing} chunks already loaded")

    total_chunks = 0

    # Each subdirectory = one company
    company_dirs = [d for d in PDF_DIR.iterdir() if d.is_dir()]
    print(f"\nFound {len(company_dirs)} companies: {[d.name for d in company_dirs]}")

    for company_dir in sorted(company_dirs):
        company = company_dir.name
        archives = list(company_dir.glob("*.zip")) + list(company_dir.glob("*.rar"))
        loose_pdfs = [p for p in company_dir.glob("*.pdf")]

        if not archives and not loose_pdfs:
            print(f"\n  [{company}] no files found — skipping")
            continue

        print(f"\n  [{company}] {len(archives)} archives, {len(loose_pdfs)} loose PDFs")

        # Process loose PDFs directly (already extracted)
        for pdf_path in sorted(loose_pdfs):
            year = extract_year(pdf_path.stem)
            print(f"    {pdf_path.name}  (year={year})")
            chunks, ids, metas = parse_pdf(pdf_path, company, year)
            if not chunks:
                print(f"      No text extracted")
                continue
            batch_size = 64
            for i in range(0, len(chunks), batch_size):
                embeddings = model.encode(chunks[i:i+batch_size], show_progress_bar=False).tolist()
                collection.upsert(ids=ids[i:i+batch_size], documents=chunks[i:i+batch_size],
                                  embeddings=embeddings, metadatas=metas[i:i+batch_size])
            total_chunks += len(chunks)
            print(f"      {len(chunks)} chunks")

        # Process archives (skip if a loose PDF with same year already processed)
        loose_years = {extract_year(p.stem) for p in loose_pdfs}
        for archive in sorted(archives):
            year = extract_year(archive.stem)
            if year in loose_years and year != 0:
                print(f"    {archive.name}  — skipped (loose PDF already loaded)")
                continue
            print(f"    {archive.name}  (year={year})")

            with tempfile.TemporaryDirectory() as tmpdir:
                try:
                    pdfs = extract_archive(archive, Path(tmpdir))
                except Exception as e:
                    print(f"      Extract error: {e}")
                    continue

                if not pdfs:
                    print(f"      No PDFs found inside archive")
                    continue

                for pdf_path in pdfs:
                    chunks, ids, metas = parse_pdf(pdf_path, company, year)
                    if not chunks:
                        print(f"      No text extracted from {pdf_path.name}")
                        continue

                    # Batch embed and upsert
                    batch_size = 64
                    for i in range(0, len(chunks), batch_size):
                        b_texts = chunks[i:i + batch_size]
                        b_ids   = ids[i:i + batch_size]
                        b_metas = metas[i:i + batch_size]
                        embeddings = model.encode(b_texts, show_progress_bar=False).tolist()
                        collection.upsert(
                            ids=b_ids,
                            documents=b_texts,
                            embeddings=embeddings,
                            metadatas=b_metas,
                        )

                    total_chunks += len(chunks)
                    print(f"      {pdf_path.name}: {len(chunks)} chunks")

    print(f"\nDone. Added {total_chunks} new chunks.")
    print(f"Collection '{COLLECTION_NAME}' total: {collection.count()} chunks")


if __name__ == "__main__":
    load_corp_reports()
