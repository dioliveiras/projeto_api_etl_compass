# src/etl/gold.py
from __future__ import annotations

from typing import Dict
import pandas as pd

def build_gold_views(df_enriched: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Cria DataFrames para a camada Gold.
    - enriched_latest: snapshot do último dia disponível (1 linha por país)
    - country_timeseries: histórico de cotações por país (date × país)
    """
    if df_enriched is None or df_enriched.empty:
        raise ValueError("df_enriched vazio para camada Gold.")

    df = df_enriched.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "currency_code", "rate_to_base"]).reset_index(drop=True)

    # 1) Snapshot do último dia (por país)
    last_day = df["date"].max()
    latest = (
        df[df["date"] == last_day]
        .sort_values(["region", "country_name", "currency_code"])
        [["date","country_name","region","subregion","currency_code","rate_to_base","base","cca3"]]
        .reset_index(drop=True)
    )
    latest["date"] = latest["date"].dt.strftime("%Y-%m-%d")

    # 2) Histórico por país (sem agregação)
    country_ts = (
        df.sort_values(["date","region","country_name","currency_code"])
          [["date","country_name","region","subregion","currency_code","rate_to_base","base","cca3"]]
          .reset_index(drop=True)
    )
    country_ts["date"] = country_ts["date"].dt.strftime("%Y-%m-%d")

    return {
        "enriched_latest": latest,
        "country_timeseries": country_ts,
    }