"""Semantic embedding-based news features for signal detection.

Replaces keyword-count features (news_daily) with semantic similarity scores.
For each (date, topic), computes:
  1. Embeddings of articles matching topic keywords
  2. Embedding of topic keywords
  3. Cosine similarity scores
  4. Weighted-mean daily aggregation

Input: articles from hf_news.db or query result
Output: DataFrame with columns [news_date, oil_emb, rate_emb, ..., gold_emb]

Append-only: only computes dates > existing MAX(news_date) in DB.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import logging

import numpy as np
import pandas as pd
import duckdb
from sentence_transformers import SentenceTransformer
from scipy.spatial.distance import cosine

logger = logging.getLogger(__name__)

MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
AGGREGATION_METHOD = "weighted_mean"
AGGREGATION_TAU = 0.1  # temperature for softmax weighting

TOPICS = {
    "oil":        ["oil", "Brent", "crude", "energy", "нефть", "нефт", "газ"],
    "rate":       ["interest rate", "central bank", "key rate", "CBR",
                   "ставк", "центробанк", "ключевая ставка"],
    "ruble":      ["ruble", "RUB", "Russian currency",
                   "рубл", "курс рубля"],
    "sanctions":  ["sanction", "embargo", "Russia ban",
                   "санкци", "эмбарго"],
    "inflation":  ["inflation", "CPI", "consumer price",
                   "инфляц", "потребительские цены"],
    "banking":    ["banking", "bank sector", "financial sector",
                   "банк", "банковск", "кредит"],
    "gold":       ["gold", "precious metal",
                   "золото", "золот"],
}

DB_PATH = Path(__file__).parents[2] / "db" / "signal_mind.duckdb"
NEWS_PATH = Path(__file__).parents[2] / "db" / "hf_news.db"


class EmbeddingFeatureBuilder:
    """Builds semantic embedding-based daily news features."""

    def __init__(self, model_name: str = MODEL_NAME, batch_size: int = 32):
        """
        Args:
            model_name: HuggingFace model ID (multilingual-capable)
            batch_size: batch size for encoding (trade-off between speed and memory)
        """
        self.model_name = model_name
        self.batch_size = batch_size
        logger.info(f"Loading model: {model_name}")
        self.model = SentenceTransformer(model_name)

        # Precompute topic keyword embeddings
        self.topic_embeddings = {}
        for topic, keywords in TOPICS.items():
            topic_text = " ".join(keywords)
            emb = self.model.encode([topic_text], convert_to_numpy=True)[0]
            self.topic_embeddings[topic] = emb
            logger.debug(f"  {topic}: embedding shape {emb.shape}")

    def load_articles_for_topic(self,
                                conn: duckdb.DuckDBPyConnection,
                                topic: str,
                                date_start: str,
                                date_end: str) -> list[tuple[str, str]]:
        """
        Load articles from hf_news.db matching topic keywords.

        Args:
            conn: DuckDB connection (must have hf_news.db attached as 'news')
            topic: topic name (key in TOPICS)
            date_start, date_end: date range (YYYY-MM-DD)

        Returns:
            List of (date, title) tuples
        """
        keywords = TOPICS[topic]
        ors = " OR ".join(f"text ILIKE '%{kw}%'" for kw in keywords)

        query = f"""
            SELECT date, text
            FROM news.articles
            WHERE date BETWEEN '{date_start}' AND '{date_end}'
              AND ({ors})
            ORDER BY date
        """
        try:
            result = conn.execute(query).fetchall()
            return result
        except Exception as e:
            logger.warning(f"Failed to load articles for {topic}: {e}")
            return []

    def aggregate_daily_score(self, similarities: list[float], method: str = "weighted_mean") -> float:
        """
        Aggregate per-article similarity scores into a single daily score.

        Args:
            similarities: list of cosine similarity values in [0, 1]
            method: aggregation method ("mean", "max", "weighted_mean")

        Returns:
            Daily score in [0, 1]
        """
        if not similarities:
            return 0.0

        similarities = np.array(similarities)
        if method == "mean":
            return float(np.mean(similarities))
        elif method == "max":
            return float(np.max(similarities))
        elif method == "weighted_mean":
            # Softmax weighting: higher scores get higher weight
            scores = np.clip(similarities, 0, 1)
            if len(scores) == 1:
                return float(scores[0])
            weights = np.exp(scores / AGGREGATION_TAU)
            weights /= weights.sum()
            return float(np.dot(weights, scores))
        else:
            raise ValueError(f"Unknown aggregation method: {method}")

    def build_daily_scores(self,
                          conn: duckdb.DuckDBPyConnection,
                          date_start: Optional[str] = None,
                          date_end: Optional[str] = None,
                          batch_topics: Optional[list[str]] = None) -> pd.DataFrame:
        """
        Build daily embedding-based feature scores.

        If date_start is None, computes from MAX(news_date) + 1 day in existing table.
        If date_end is None, uses today's date.

        Args:
            conn: DuckDB connection with hf_news.db attached
            date_start: start date (YYYY-MM-DD), or None for append mode
            date_end: end date (YYYY-MM-DD), or None for today
            batch_topics: subset of topics to compute, or None for all

        Returns:
            DataFrame with columns [news_date, oil_emb, rate_emb, ..., gold_emb]
        """
        topics_to_compute = batch_topics or list(TOPICS.keys())

        # Determine date range
        if date_start is None:
            max_row = conn.execute("SELECT MAX(news_date) FROM news_daily").fetchone()
            max_date = max_row[0] if max_row and max_row[0] else None
            if max_date is None:
                date_start = "2021-01-01"
            else:
                date_start = (max_date + timedelta(days=1)).isoformat()

        if date_end is None:
            date_end = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"Building embeddings for {date_start} to {date_end}")

        # Collect all dates in range
        all_dates = pd.date_range(date_start, date_end, freq='D')

        # For each date, compute topic embeddings
        results = []

        for idx, date_obj in enumerate(all_dates):
            if idx % 50 == 0:
                logger.info(f"  processing {idx}/{len(all_dates)}")

            date_str = date_obj.strftime("%Y-%m-%d")
            row = {"news_date": date_str}

            for topic in topics_to_compute:
                articles = self.load_articles_for_topic(
                    conn, topic, date_str, date_str
                )

                if not articles:
                    row[f"{topic}_emb"] = 0.0
                    continue

                # Extract text snippets (title + first 200 chars for richness)
                texts = []
                for date, full_text in articles:
                    title = full_text.split('\n')[0] if '\n' in full_text else full_text[:100]
                    texts.append(title)

                # Embed articles in batch
                try:
                    article_embeddings = self.model.encode(
                        texts, convert_to_numpy=True, batch_size=self.batch_size
                    )
                except Exception as e:
                    logger.warning(f"Embedding failed for {topic}/{date_str}: {e}. Fallback to 0.0")
                    row[f"{topic}_emb"] = 0.0
                    continue

                # Compute similarities to topic embedding
                topic_emb = self.topic_embeddings[topic]
                similarities = []
                for article_emb in article_embeddings:
                    sim = 1.0 - cosine(article_emb, topic_emb)
                    similarities.append(max(0.0, sim))  # clamp to [0, 1]

                # Aggregate
                daily_score = self.aggregate_daily_score(similarities, AGGREGATION_METHOD)
                row[f"{topic}_emb"] = daily_score

            results.append(row)

        df = pd.DataFrame(results)
        logger.info(f"Built {len(df)} daily rows for {len(topics_to_compute)} topics")
        return df

    def save_to_duckdb(self, df: pd.DataFrame, overwrite: bool = False) -> None:
        """
        Save embedding features to signal_mind.duckdb.

        Append-only by default (fails if dates overlap). Set overwrite=True to
        replace existing dates (NOT recommended for production).

        Args:
            df: DataFrame with columns [news_date, oil_emb, rate_emb, ...]
            overwrite: if True, delete conflicting dates first
        """
        logger.info(f"Writing {len(df)} embedding rows to {DB_PATH}")

        con = duckdb.connect(str(DB_PATH))

        # Ensure embedding columns exist
        topic_cols = list(TOPICS.keys())
        for topic in topic_cols:
            col_name = f"{topic}_emb"
            try:
                con.execute(f"ALTER TABLE news_daily ADD COLUMN {col_name} DOUBLE")
            except duckdb.CatalogException:
                pass  # column already exists

        # Handle conflicts
        if not overwrite:
            existing = con.execute(
                f"SELECT COUNT(*) FROM news_daily "
                f"WHERE news_date IN (SELECT news_date FROM ({df.to_sql('temp_emb', con)})"
            ).fetchone()[0]
            if existing > 0:
                logger.error(f"Conflict: {existing} dates already exist. Set overwrite=True to replace.")
                con.close()
                return
        else:
            dates_to_delete = df["news_date"].tolist()
            placeholders = ",".join(f"'{d}'" for d in dates_to_delete)
            con.execute(f"DELETE FROM news_daily WHERE news_date IN ({placeholders})")
            logger.warning(f"Deleted {len(dates_to_delete)} existing dates")

        # Insert embeddings
        topic_cols_str = ", ".join(f"{topic}_emb" for topic in topic_cols)
        for _, row in df.iterrows():
            date = row["news_date"]
            values = ", ".join(
                f"{row.get(f'{topic}_emb', 0.0)}" for topic in topic_cols
            )
            con.execute(
                f"UPDATE news_daily SET {topic_cols_str} = ({values}) "
                f"WHERE news_date = '{date}'"
            )

        con.commit()
        con.close()
        logger.info("Write complete")


def build_embeddings_main(date_start: Optional[str] = None,
                          date_end: Optional[str] = None,
                          save: bool = True) -> pd.DataFrame:
    """
    Main entry point: build and optionally save embedding features.

    Args:
        date_start: start date or None for append mode
        date_end: end date or None for today
        save: if True, write to DB

    Returns:
        DataFrame with embedding features
    """
    builder = EmbeddingFeatureBuilder()

    con = duckdb.connect(str(DB_PATH))
    con.execute(f"ATTACH '{NEWS_PATH}' AS news (TYPE sqlite)")

    df = builder.build_daily_scores(con, date_start, date_end)

    if save:
        builder.save_to_duckdb(df)

    con.close()
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = build_embeddings_main(save=True)
    print(f"\nBuilt {len(df)} rows")
    print(df.head())
