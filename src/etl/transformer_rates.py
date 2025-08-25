# src/etl/transformer_rates.py
from __future__ import annotations

import logging
from typing import Tuple, Dict, Any
import pandas as pd

logger = logging.getLogger("etl.transformer_rates")

PRIMARY_COLS = ["date", "currency_code", "rate_to_base", "base"]

def transform_rates(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Normaliza e valida a timeseries de câmbio.
    """
    if df is None or df.empty:
        raise ValueError("Transformer de rates recebeu DataFrame vazio.")

    df = df.copy()
    # Tipagem/coerção
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["currency_code"] = df["currency_code"].astype("string")
    df["rate_to_base"] = pd.to_numeric(df["rate_to_base"], errors="coerce").astype("float64")
    df["base"] = df["base"].astype("string")

    # Remoção de nulos críticos
    df = df.dropna(subset=["date", "currency_code", "rate_to_base"])

    # Colunas garantidas
    for c in PRIMARY_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[PRIMARY_COLS].sort_values(["date", "currency_code"]).reset_index(drop=True)

    quality = {
        "rows": int(len(df)),
        "nulls_per_column": {c: int(df[c].isna().sum()) for c in df.columns},
        "unique_currencies": int(df["currency_code"].nunique()),
        "date_min": df["date"].min().strftime("%Y-%m-%d") if len(df) else None,
        "date_max": df["date"].max().strftime("%Y-%m-%d") if len(df) else None,
        "sample": df.head(3).to_dict(orient="records"),
    }
    logger.info("Transformer rates: linhas=%s | moedas=%s | %s→%s",
                quality["rows"], quality["unique_currencies"], quality["date_min"], quality["date_max"])
    return df, quality
