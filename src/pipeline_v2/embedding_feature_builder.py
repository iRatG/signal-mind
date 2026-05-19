"""Semantic embedding-based news features for signal detection.

Architecture (efficient):
  1. Load ALL articles for the date range in ONE query (no topic filter)
  2. Encode all articles once with SentenceTransformer
  3. Save embeddings cache to disk (NPZ) — reusable across runs
  4. For each topic: filter articles by keyword, lookup cached embeddings,
     compute cosine similarity, aggregate per date
  5. Write as new *_emb columns in news_daily via UPDATE JOIN

This way the expensive encode step runs once, not 7×.

Output columns added to news_daily:
  oil_emb, rate_emb, ruble_emb, sanctions_emb, inflation_emb, banking_emb, gold_emb

Usage:
    .venv/Scripts/python -m src.pipeline_v2.embedding_feature_builder \\
        --date-start 2022-01-01 --date-end 2023-09-30
    # Add --dry-run to preview without writing to DB
"""
from __future__ import annotations

import argparse
import logging
import pickle
import time
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import duckdb
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────

MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"

# Softmax temperature for weighted-mean aggregation
AGGREGATION_TAU = 0.1

# Batch size for encoding (tradeoff: memory vs. speed)
ENCODE_BATCH_SIZE = 64

# Article chunk size for loading (avoid loading 2.56M rows at once)
LOAD_CHUNK_DAYS = 90   # process 3 months at a time

# Cache directory for embeddings (so re-runs are fast)
CACHE_DIR = Path(__file__).parents[2] / "db" / "emb_cache"

TOPICS: dict[str, list[str]] = {
    "oil":       ["oil", "Brent", "crude", "energy", "нефть", "нефт", "газ"],
    "rate":      ["interest rate", "central bank", "key rate", "CBR",
                  "ставк", "центробанк", "ключевая ставка"],
    "ruble":     ["ruble", "RUB", "Russian currency",
                  "рубл", "курс рубля"],
    "sanctions": ["sanction", "embargo", "Russia ban",
                  "санкци", "эмбарго"],
    "inflation": ["inflation", "CPI", "consumer price",
                  "инфляц", "потребительские цены"],
    "banking":   ["banking", "bank sector", "financial sector",
                  "банк", "банковск", "кредит"],
    "gold":      ["gold", "precious metal",
                  "золото", "золот"],
}

DB_PATH   = Path(__file__).parents[2] / "db" / "signal_mind.duckdb"
NEWS_PATH = Path(__file__).parents[2] / "db" / "hf_news.db"


# ──────────────────────────────────────────────────────────────────────────────
# Core builder
# ──────────────────────────────────────────────────────────────────────────────

class EmbeddingFeatureBuilder:

    def __init__(self, model_name: str = MODEL_NAME):
        t0 = time.time()
        logger.info(f"Loading model: {model_name}")
        try:
            self.model = SentenceTransformer(model_name, local_files_only=True)
        except Exception:
            self.model = SentenceTransformer(model_name)
        logger.info(f"  loaded in {time.time()-t0:.1f}s")

        try:
            self.emb_dim: int = self.model.get_embedding_dimension()
        except AttributeError:
            self.emb_dim = self.model.get_sentence_embedding_dimension()

        # Precompute topic query embeddings once
        self.topic_embs: dict[str, np.ndarray] = {}
        for topic, keywords in TOPICS.items():
            text = " ".join(keywords)
            self.topic_embs[topic] = self.model.encode(
                [text], convert_to_numpy=True, show_progress_bar=False
            )[0]

        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # ─── load all articles for a date chunk ────────────────────────────────────

    def _load_chunk(self,
                    news_conn: duckdb.DuckDBPyConnection,
                    date_start: str,
                    date_end: str) -> pd.DataFrame:
        """
        Load article IDs, dates, and text snippets (title only).
        No topic filter — loads all articles in the date range.
        """
        query = f"""
            SELECT
                id,
                date,
                CASE
                    WHEN POSITION(chr(10) IN text) > 0
                    THEN SUBSTRING(text, 1, POSITION(chr(10) IN text) - 1)
                    ELSE SUBSTRING(text, 1, 150)
                END AS title,
                text
            FROM news.articles
            WHERE date BETWEEN '{date_start}' AND '{date_end}'
            ORDER BY date, id
        """
        try:
            return news_conn.execute(query).df()
        except Exception as e:
            logger.error(f"Load failed: {e}")
            return pd.DataFrame(columns=["id", "date", "title", "text"])

    # ─── encode + cache ────────────────────────────────────────────────────────

    def _encode_or_load_cache(self,
                               df_chunk: pd.DataFrame,
                               chunk_key: str) -> np.ndarray:
        """
        Encode article titles. Loads from NPZ cache if available.
        Cache key = chunk_key (e.g. "2022-01_2022-03").

        Returns: (N, dim) float32 array aligned with df_chunk rows.
        """
        cache_file = CACHE_DIR / f"emb_{chunk_key}.npz"
        id_file    = CACHE_DIR / f"ids_{chunk_key}.pkl"

        # Check cache: same IDs in same order → reuse
        if cache_file.exists() and id_file.exists():
            cached_ids = pickle.loads(id_file.read_bytes())
            if list(cached_ids) == list(df_chunk["id"].values):
                logger.info(f"  cache HIT: {cache_file.name}")
                return np.load(cache_file)["embs"]
            else:
                logger.info(f"  cache MISS (IDs differ) — re-encoding")

        # Encode
        titles = df_chunk["title"].fillna("").tolist()
        logger.info(f"  encoding {len(titles)} articles...")
        t0 = time.time()
        embs = self.model.encode(
            titles,
            batch_size=ENCODE_BATCH_SIZE,
            convert_to_numpy=True,
            show_progress_bar=True,
        )
        logger.info(f"  encoded in {time.time()-t0:.1f}s")

        # Save cache
        np.savez_compressed(str(cache_file), embs=embs)
        id_file.write_bytes(pickle.dumps(df_chunk["id"].values))
        logger.info(f"  saved cache: {cache_file.name}")

        return embs

    # ─── per-topic similarity ──────────────────────────────────────────────────

    @staticmethod
    def _cosine_sim_batch(article_embs: np.ndarray, topic_emb: np.ndarray) -> np.ndarray:
        """Vectorised cosine similarity → (N,) clipped to [0, 1]."""
        norms = np.linalg.norm(article_embs, axis=1)
        norm_t = np.linalg.norm(topic_emb)
        sims = (article_embs @ topic_emb) / np.maximum(norms * norm_t, 1e-9)
        return np.clip(sims, 0.0, 1.0)

    @staticmethod
    def _weighted_mean(scores: np.ndarray, tau: float = AGGREGATION_TAU) -> float:
        if len(scores) == 0:
            return 0.0
        if len(scores) == 1:
            return float(scores[0])
        exp_s = np.exp(np.clip(scores / tau, -50, 50))
        return float(np.dot(exp_s / exp_s.sum(), scores))

    def _topic_daily_scores(self,
                             df_chunk: pd.DataFrame,
                             embs: np.ndarray,
                             topic: str) -> pd.Series:
        """
        Filter articles for topic, compute similarity, aggregate per date.

        Returns: pd.Series(index=date, name=f"{topic}_emb")
        """
        keywords = TOPICS[topic]
        # Fast keyword filter in pandas (OR of ILIKE → case-insensitive contains)
        pattern = "|".join(kw.lower() for kw in keywords)
        mask = df_chunk["text"].str.lower().str.contains(pattern, na=False, regex=True)
        df_topic = df_chunk[mask].copy()

        if df_topic.empty:
            return pd.Series(dtype=float, name=f"{topic}_emb")

        topic_emb = self.topic_embs[topic]
        sims = self._cosine_sim_batch(embs[mask.values], topic_emb)
        df_topic["sim"] = sims

        def _agg(grp: pd.Series) -> float:
            return self._weighted_mean(grp.values)

        result = df_topic.groupby("date")["sim"].agg(_agg)
        result.name = f"{topic}_emb"
        return result

    # ─── main build loop ───────────────────────────────────────────────────────

    def build_daily_scores(self,
                           news_conn: duckdb.DuckDBPyConnection,
                           date_start: str,
                           date_end: str,
                           topics: Optional[list[str]] = None) -> pd.DataFrame:
        """
        Build daily embedding features for all topics in [date_start, date_end].

        Processes in 90-day chunks to keep memory manageable.
        Uses on-disk embedding cache for speed.

        Returns DataFrame with columns [news_date, oil_emb, ..., gold_emb].
        """
        topics = topics or list(TOPICS.keys())
        topic_cols = [f"{t}_emb" for t in topics]
        logger.info(f"Building embeddings: {date_start} → {date_end}")

        chunks = pd.date_range(date_start, date_end, freq=f"{LOAD_CHUNK_DAYS}D")
        dates_end = list(chunks[1:].strftime("%Y-%m-%d")) + [date_end]
        chunks_start = chunks.strftime("%Y-%m-%d")

        chunk_series: dict[str, list[pd.Series]] = {t: [] for t in topics}

        for cs, ce in zip(chunks_start, dates_end):
            logger.info(f"\nChunk: {cs} → {ce}")
            df_chunk = self._load_chunk(news_conn, cs, ce)
            if df_chunk.empty:
                logger.info("  no articles in this chunk")
                continue

            logger.info(f"  loaded {len(df_chunk)} articles")

            chunk_key = f"{cs[:7]}_{ce[:7]}"
            embs = self._encode_or_load_cache(df_chunk, chunk_key)

            for topic in topics:
                s = self._topic_daily_scores(df_chunk, embs, topic)
                chunk_series[topic].append(s)

        # Concat all chunks per topic
        combined: dict[str, pd.Series] = {}
        for topic in topics:
            parts = chunk_series[topic]
            if parts:
                combined[topic] = pd.concat(parts).groupby(level=0).first()
            else:
                combined[topic] = pd.Series(dtype=float, name=f"{topic}_emb")

        if not any(len(s) > 0 for s in combined.values()):
            logger.warning("No embedding scores computed — check news_path and date range")
            return pd.DataFrame(columns=["news_date"] + topic_cols)

        df = pd.DataFrame({f"{t}_emb": combined[t] for t in topics})
        df.index.name = "news_date"
        df = df.reset_index()
        df["news_date"] = pd.to_datetime(df["news_date"]).dt.date
        df = df.fillna(0.0)

        logger.info(f"\nTotal: {len(df)} daily rows")
        return df

    # ─── persist ───────────────────────────────────────────────────────────────

    def save_to_duckdb(self, df: pd.DataFrame) -> int:
        """
        Add embedding columns to news_daily and update rows.
        Only updates rows where news_date already exists (no inserts).

        Returns: number of rows updated.
        """
        if df.empty:
            logger.warning("Empty DataFrame — nothing written")
            return 0

        con = duckdb.connect(str(DB_PATH))

        # Add columns if missing
        for topic in TOPICS:
            col = f"{topic}_emb"
            try:
                con.execute(f"ALTER TABLE news_daily ADD COLUMN {col} DOUBLE DEFAULT 0.0")
                logger.info(f"  added column {col}")
            except duckdb.CatalogException:
                pass

        # Register and UPDATE via JOIN
        con.register("emb_df", df)

        available_topics = [t for t in TOPICS if f"{t}_emb" in df.columns]
        set_clause = ",\n    ".join(
            f"news_daily.{t}_emb = emb_df.{t}_emb"
            for t in available_topics
        )
        con.execute(f"""
            UPDATE news_daily
            SET {set_clause}
            FROM emb_df
            WHERE news_daily.news_date = emb_df.news_date::DATE
        """)

        placeholders = ", ".join(f"'{d}'" for d in df["news_date"].astype(str).tolist())
        updated = con.execute(
            f"SELECT COUNT(*) FROM news_daily WHERE news_date IN ({placeholders})"
        ).fetchone()[0]

        con.unregister("emb_df")
        con.commit()
        con.close()

        logger.info(f"Updated {updated} rows in news_daily")
        return updated


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Build embedding-based news features")
    parser.add_argument("--date-start", default="2022-01-01",
                        help="Start date YYYY-MM-DD (default: 2022-01-01 = Train start)")
    parser.add_argument("--date-end",   default="2025-04-30",
                        help="End date YYYY-MM-DD (default: 2025-04-30 = Val end)")
    parser.add_argument("--topics", nargs="+", default=None,
                        help="Topic subset, e.g. --topics oil rate")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without writing to DB")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    builder = EmbeddingFeatureBuilder()

    # Use in-memory DuckDB just for reading hf_news.db (SQLite attach)
    # signal_mind.duckdb is opened separately only if we need to save
    con = duckdb.connect(":memory:")
    con.execute(f"ATTACH '{NEWS_PATH}' AS news (TYPE sqlite)")

    df = builder.build_daily_scores(
        con,
        date_start=args.date_start,
        date_end=args.date_end,
        topics=args.topics,
    )
    con.close()

    print(f"\n{'='*60}")
    print(f"Built {len(df)} rows × {len(df.columns)} columns")
    if not df.empty:
        print(df.describe().round(4))
        print(f"\nSample:\n{df.head(5).to_string()}")
    print(f"{'='*60}")

    if not args.dry_run:
        n = builder.save_to_duckdb(df)
        print(f"\nWrote to news_daily: {n} rows updated")
    else:
        print("\n--dry-run: DB not modified")


if __name__ == "__main__":
    main()
