"""Feature transformation utilities for signal search.

Applies to DataFrames that already contain news columns (keyword counts or
embedding scores). Adds normalized variants and alternative targets.

Usage (inside night_search.py):
    df = load_split(...)
    df = FeatureTransformer.apply(df, feature_type="keyword_z90", target="vol_5d")
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

KEYWORD_TOPICS = ["oil", "rate", "ruble", "sanctions", "inflation", "banking", "gold"]
EMBEDDING_TOPICS = [f"{t}_emb" for t in KEYWORD_TOPICS]

FEATURE_TYPES = [
    "keyword_raw",      # raw keyword counts (baseline)
    "keyword_z90",      # rolling z-score, window=90
    "keyword_z30",      # rolling z-score, window=30
    "embedding_raw",    # raw cosine similarity scores
    "embedding_z90",    # normalized embedding scores, window=90
]

TARGETS = [
    "market_return",    # log-return (baseline)
    "abs_return",       # |log-return| — directional strength proxy
    "vol_5d",           # 5-day realized volatility (annualised)
]


# ──────────────────────────────────────────────────────────────────────────────
# Transformer
# ──────────────────────────────────────────────────────────────────────────────

class FeatureTransformer:

    @staticmethod
    def rolling_zscore(series: pd.Series, window: int, min_periods: int = 20) -> pd.Series:
        """Rolling z-score. Clips at ±5 to suppress outliers."""
        mu  = series.rolling(window, min_periods=min_periods).mean()
        sig = series.rolling(window, min_periods=min_periods).std().clip(lower=1e-6)
        return ((series - mu) / sig).clip(-5.0, 5.0)

    @staticmethod
    def add_volatility_targets(df: pd.DataFrame) -> pd.DataFrame:
        """Add abs_return and vol_5d columns. Requires market_return column."""
        if "market_return" not in df.columns:
            return df
        df = df.copy()
        df["abs_return"] = df["market_return"].abs()
        df["vol_5d"] = (
            df["market_return"]
            .rolling(5, min_periods=3).std()
            .mul(np.sqrt(252))
            .clip(lower=0.0)
        )
        return df

    @classmethod
    def apply(cls, df: pd.DataFrame, feature_type: str, target: str) -> pd.DataFrame:
        """
        Transform df in-place (copy) according to feature_type and target.

        Feature columns are renamed to a canonical set so the scanner loop
        can use the same topic names regardless of feature_type:
          - keyword_raw     → columns: oil, rate, ...
          - keyword_z90/z30 → columns: oil_z, rate_z, ...
          - embedding_raw   → columns: oil_emb (already present)
          - embedding_z90   → columns: oil_emb_z, ...

        Returns df with:
          - topic columns according to feature_type
          - target column = target param (market_return / abs_return / vol_5d)
          - key_rate_pct (unchanged, used by M5 as confounder)
        """
        df = df.copy()

        # 1. Add volatility targets
        df = cls.add_volatility_targets(df)

        # 2. Feature normalization
        if feature_type == "keyword_raw":
            # No change — use raw counts
            pass

        elif feature_type == "keyword_z90":
            for t in KEYWORD_TOPICS:
                if t in df.columns:
                    df[f"{t}_z"] = cls.rolling_zscore(df[t], window=90)

        elif feature_type == "keyword_z30":
            for t in KEYWORD_TOPICS:
                if t in df.columns:
                    df[f"{t}_z"] = cls.rolling_zscore(df[t], window=30)

        elif feature_type == "embedding_raw":
            # Already present as oil_emb etc.
            pass

        elif feature_type == "embedding_z90":
            for t in KEYWORD_TOPICS:
                col = f"{t}_emb"
                if col in df.columns:
                    df[f"{t}_emb_z"] = cls.rolling_zscore(df[col], window=90)

        # 3. Verify target exists
        if target not in df.columns:
            raise ValueError(f"Target '{target}' not in DataFrame after transform. "
                             f"Available: {list(df.columns)}")

        return df

    @staticmethod
    def topic_columns(feature_type: str) -> list[str]:
        """Return the column names to use for this feature_type."""
        mapping = {
            "keyword_raw":   KEYWORD_TOPICS,
            "keyword_z90":   [f"{t}_z"     for t in KEYWORD_TOPICS],
            "keyword_z30":   [f"{t}_z"     for t in KEYWORD_TOPICS],
            "embedding_raw": [f"{t}_emb"   for t in KEYWORD_TOPICS],
            "embedding_z90": [f"{t}_emb_z" for t in KEYWORD_TOPICS],
        }
        return mapping[feature_type]
