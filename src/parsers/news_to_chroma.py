"""Stream Russian financial news from HuggingFace → filter → ChromaDB.

Dataset: RUParaCorpus or IlyaGusev/ru_news — streamed to avoid RAM overflow.
Filtered by financial keywords, embedded and stored in ChromaDB.
"""
import hashlib
import time
from pathlib import Path

import chromadb
from datasets import load_dataset
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

CHROMA_DIR = Path(__file__).parents[2] / "db" / "chroma"
MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
COLLECTION_NAME = "news_ru"

# HuggingFace dataset — financial/economic Russian news
DATASET_NAME = "IlyaGusev/ru_news"

BATCH_SIZE = 64
MAX_ARTICLES = 200_000   # hard cap to avoid multi-day run; increase later

KEYWORDS = [
    # макро
    "ключевая ставка", "инфляция", "цб рф", "центробанк", "рубль", "курс доллара",
    "ввп", "экономика", "рецессия", "кризис",
    # рынки
    "акции", "облигации", "биржа", "моex", "индекс", "нефть", "brent", "золото",
    "фондовый рынок", "дивиденды", "ipo",
    # компании
    "сбербанк", "газпром", "лукойл", "роснефть", "яндекс", "втб", "норникель",
    # регулирование
    "цб", "банк России", "мфо", "микрофинанс", "санкции", "ограничения",
    # недвижимость
    "ипотека", "жилье", "строительство", "недвижимость",
    # труд
    "зарплата", "безработица", "занятость",
]


def matches_keywords(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in KEYWORDS)


def article_id(text: str) -> str:
    return hashlib.md5(text[:200].encode("utf-8", errors="ignore")).hexdigest()


def load_news():
    CHROMA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Loading embedding model: {MODEL_NAME} ...")
    model = SentenceTransformer(MODEL_NAME)

    client = chromadb.PersistentClient(path=str(CHROMA_DIR))
    collection = client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},
    )

    print(f"Streaming dataset: {DATASET_NAME} ...")
    dataset = load_dataset(DATASET_NAME, split="train", streaming=True, trust_remote_code=True)

    batch_texts, batch_ids, batch_metas = [], [], []
    total_seen = 0
    total_saved = 0
    start = time.time()

    for article in tqdm(dataset, desc="Streaming"):
        total_seen += 1

        title = str(article.get("title") or "")
        text = str(article.get("text") or article.get("content") or "")
        full = f"{title} {text}"

        if not matches_keywords(full):
            continue

        # Truncate to 1000 chars for embedding
        chunk = full[:1000].strip()
        if len(chunk) < 50:
            continue

        aid = article_id(chunk)
        batch_texts.append(chunk)
        batch_ids.append(aid)
        batch_metas.append({
            "title": title[:200],
            "source": str(article.get("source") or ""),
            "date": str(article.get("date") or article.get("published") or ""),
            "url": str(article.get("url") or "")[:300],
        })

        if len(batch_texts) >= BATCH_SIZE:
            embeddings = model.encode(batch_texts, show_progress_bar=False).tolist()
            collection.upsert(ids=batch_ids, documents=batch_texts,
                              embeddings=embeddings, metadatas=batch_metas)
            total_saved += len(batch_texts)
            batch_texts, batch_ids, batch_metas = [], [], []

            elapsed = time.time() - start
            print(f"  seen={total_seen:,}  saved={total_saved:,}  "
                  f"elapsed={elapsed/60:.1f}m  rate={total_seen/elapsed:.0f}/s")

        if total_saved >= MAX_ARTICLES:
            print(f"Reached cap of {MAX_ARTICLES} articles.")
            break

    # flush remainder
    if batch_texts:
        embeddings = model.encode(batch_texts, show_progress_bar=False).tolist()
        collection.upsert(ids=batch_ids, documents=batch_texts,
                          embeddings=embeddings, metadatas=batch_metas)
        total_saved += len(batch_texts)

    print(f"\nDone. Seen={total_seen:,}  Saved={total_saved:,}")
    print(f"Collection size: {collection.count()}")


if __name__ == "__main__":
    load_news()
