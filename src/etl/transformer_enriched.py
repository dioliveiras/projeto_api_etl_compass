# src/etl/transformer_enriched.py
from __future__ import annotations

import logging
from typing import Tuple, Dict, Any
import pandas as pd

logger = logging.getLogger("etl.transformer_enriched")

PRIMARY_COLS = [
    "date", "currency_code", "rate_to_base", "base",
    "cca3", "country_name", "region", "subregion"
]

def enrich_countries_with_rates(
    countries_silver: pd.DataFrame,
    rates_silver: pd.DataFrame,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Faz join simples (left) entre countries (silver) e rates (silver) via currency_code.
    """
    if countries_silver is None or countries_silver.empty:
        raise ValueError("countries_silver vazio.")
    if rates_silver is None or rates_silver.empty:
        raise ValueError("rates_silver vazio.")

    c = countries_silver.copy()
    r = rates_silver.copy()

    # Tipos consistentes
    c["currency_code"] = c["currency_code"].astype("string")
    r["currency_code"] = r["currency_code"].astype("string")

    cols_c = ["cca3", "country_name", "region", "subregion", "currency_code"]
    cols_r = ["date", "currency_code", "rate_to_base", "base"]

    df = r[cols_r].merge(c[cols_c], on="currency_code", how="left")

    # Ordena/colunas
    for col in PRIMARY_COLS:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[PRIMARY_COLS].sort_values(["date", "region", "country_name"]).reset_index(drop=True)

    quality = {
        "rows": int(len(df)),
        "nulls_per_column": {x: int(df[x].isna().sum()) for x in df.columns},
        "date_min": df["date"].min().strftime("%Y-%m-%d") if len(df) else None,
        "date_max": df["date"].max().strftime("%Y-%m-%d") if len(df) else None,
        "left_join_unmatched": int(df["cca3"].isna().sum()),
        "sample": df.head(3).to_dict(orient="records"),
    }
    logger.info("Enriched: linhas=%s | não casados pela moeda=%s",
                quality["rows"], quality["left_join_unmatched"])
    return df, quality
