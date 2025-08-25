# src/etl/transformer.py
from __future__ import annotations

import logging
from typing import Dict, Any, Tuple
import pandas as pd

logger = logging.getLogger("etl.transformer")

PRIMARY_COLS = [
    "country_name", "cca2", "cca3", "region", "subregion",
    "population", "lat", "lng", "currency_code"
]

def _extract_primary_currency(currencies: Dict[str, Any] | float | None) -> str | None:
    """
    Extrai o primeiro código de moeda do dicionário 'currencies' da REST Countries.
    Retorna None se não houver.
    """
    if not isinstance(currencies, dict) or not currencies:
        return None
    # Pega a primeira chave (ordem não garantida, mas suficiente para nosso bronze/silver)
    return next(iter(currencies.keys()), None)

def transform_countries(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    """
    Limpa/normaliza DataFrame de países.
    Retorna: (df_transformado, relatorio_qualidade)
    """
    if df is None or df.empty:
        raise ValueError("Transformer recebeu DataFrame vazio.")

    df = df.copy()

    # --- Normalizações básicas ---
    # Extrai moeda principal
    df["currency_code"] = df["currencies"].apply(_extract_primary_currency)
    # Preenche nulos textuais
    df["region"] = df["region"].fillna("Unknown")
    df["subregion"] = df["subregion"].fillna("Unknown")

    # Tipagem
    # population pode ter NaN → usar pandas Int64 (nullable)
    df["population"] = pd.to_numeric(df["population"], errors="coerce").astype("Int64")
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce").astype("float64")
    df["lng"] = pd.to_numeric(df["lng"], errors="coerce").astype("float64")

    # Strings padronizadas
    for col in ["country_name", "cca2", "cca3", "region", "subregion", "currency_code"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # Remove duplicados por cca3 (mantém a primeira ocorrência)
    before = len(df)
    df = df.sort_values(["country_name"], na_position="last").drop_duplicates(subset=["cca3"], keep="first")
    removed = before - len(df)
    if removed:
        logger.info("Transformer: %s duplicados removidos por 'cca3'.", removed)

    # Reordena colunas principais
    for col in PRIMARY_COLS:
        if col not in df.columns:
            df[col] = pd.NA  # garante presença
    df = df[PRIMARY_COLS]

    # --- Relatório simples de qualidade ---
    quality = {
        "rows": int(len(df)),
        "nulls_per_column": {c: int(df[c].isna().sum()) for c in df.columns},
        "duplicate_cca3": int(df["cca3"].duplicated().sum(skipna=False)),
        "sample": df.head(3).to_dict(orient="records"),
    }

    # Logs úteis
    logger.info(
        "Transformer: linhas=%s | nulls(cca3)=%s | nulls(country_name)=%s",
        quality["rows"],
        quality["nulls_per_column"].get("cca3", 0),
        quality["nulls_per_column"].get("country_name", 0),
    )

    return df, quality
