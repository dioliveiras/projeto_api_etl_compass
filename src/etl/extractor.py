# src/etl/extractors.py
from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable, List
import httpx
import pandas as pd

# importa e já carrega o .env
from .config import get_env  # noqa: F401

# ---------------- Endpoints
REST_COUNTRIES_URL = "https://restcountries.com/v3.1/all"
EXCHANGERATE_TS_URL = "https://api.exchangerate.host/timeframe"  # endpoint atual


# ============== REST Countries ==============
def fetch_countries() -> pd.DataFrame:
    """
    Busca lista de países da API REST Countries e retorna como DataFrame.
    Campos principais: nome, siglas, região, sub-região, moedas.
    """
    params = {
        "fields": "name,cca2,cca3,currencies,region,subregion,population,latlng"
    }
    resp = httpx.get(REST_COUNTRIES_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    for c in data:
        rows.append({
            "country_name": c.get("name", {}).get("common"),
            "cca2": c.get("cca2"),
            "cca3": c.get("cca3"),
            "region": c.get("region"),
            "subregion": c.get("subregion"),
            "population": c.get("population"),
            "lat": (c.get("latlng") or [None, None])[0],
            "lng": (c.get("latlng") or [None, None])[1],
            "currencies": c.get("currencies"),
        })
    return pd.DataFrame(rows)


def explode_currencies(df_countries: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma a coluna 'currencies' (dict) em múltiplas linhas.
    Cada país aparece uma vez por moeda.
    """
    records = []
    for _, row in df_countries.iterrows():
        currencies = row.get("currencies") or {}
        for code, meta in currencies.items():
            meta = meta or {}
            records.append({
                "cca2": row.get("cca2"),
                "cca3": row.get("cca3"),
                "country_name": row.get("country_name"),
                "region": row.get("region"),
                "subregion": row.get("subregion"),
                "currency_code": code,
                "currency_name": meta.get("name"),
                "currency_symbol": meta.get("symbol"),
            })
    return pd.DataFrame(records)


# ============== exchangerate.host ==============
def _chunk(lst: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def fetch_timeseries(
    symbols: list[str],
    start_d: date,
    end_d: date,
    base: str = "USD",          # mantido por compatibilidade; fonte costuma ser USD no plano gratuito
    api_key: str | None = None, # pode ser passado manualmente; por padrão vem do .env
    max_batch: int = 20,
    timeout_s: int = 60,
) -> pd.DataFrame:
    """
    Busca série histórica de câmbio em lotes (até 20 moedas por vez) usando /timeframe.
    Retorna colunas: date (str), currency_code, rate_to_usd (float).
    """
    # saneamento de entrada
    symbols = sorted(set([s.upper() for s in symbols if isinstance(s, str) and len(s) == 3]))
    if not symbols or start_d > end_d:
        return pd.DataFrame(columns=["date", "currency_code", "rate_to_usd"])
    if (end_d - start_d).days > 365:
        raise ValueError("Janela máxima para timeseries é 365 dias.")

    # usa access_key na query (modelo exchangerate.host)
    api_key = api_key or get_env("EXCHANGERATE_API_KEY") or None

    frames: list[pd.DataFrame] = []
    for batch in _chunk(symbols, max_batch):
        params = {
            "start_date": start_d.isoformat(),
            "end_date": end_d.isoformat(),
            "currencies": ",".join(batch),   # nome do parâmetro na /timeframe
        }
        if api_key:
            params["access_key"] = api_key  # auth via query param

        resp = httpx.get(EXCHANGERATE_TS_URL, params=params, timeout=timeout_s)
        resp.raise_for_status()
        data = resp.json()

        # Alguns erros vêm como success:false
        if isinstance(data, dict) and data.get("success") is False:
            raise RuntimeError(f"exchangerate.host retornou erro: {data.get('error')}")

        rows = []

        # 1) Formato currencylayer-like: { ... "source":"USD", "quotes": { "YYYY-MM-DD": {"USDEUR":0.85, ...}, ... } }
        if isinstance(data, dict) and isinstance(data.get("quotes"), dict):
            source = (data.get("source") or "USD").upper()
            for d_str, mapping in data["quotes"].items():
                for pair, val in (mapping or {}).items():
                    if isinstance(pair, str) and pair.upper().startswith(source) and val is not None:
                        code = pair[len(source):].upper()
                        rows.append({"date": d_str, "currency_code": code, "rate_to_usd": float(val)})

        # 2) Formato exchangerate.host clássico: { ... "rates": { "YYYY-MM-DD": {"EUR":0.85, ...}, ... } }
        elif isinstance(data, dict) and isinstance(data.get("rates"), dict):
            for d_str, mapping in data["rates"].items():
                for code, val in (mapping or {}).items():
                    if val is not None:
                        rows.append({"date": d_str, "currency_code": code.upper(), "rate_to_usd": float(val)})

        else:
            sample = str(data)[:300]
            raise RuntimeError(f"Resposta sem 'quotes' ou 'rates' válida. Amostra: {sample}")

        frames.append(pd.DataFrame(rows))

    if not frames:
        return pd.DataFrame(columns=["date", "currency_code", "rate_to_usd"])
    return pd.concat(frames, ignore_index=True)


# ============== Teste rápido (executa só direto) ==============
if __name__ == "__main__":
    # 1) País x moeda
    df_countries = fetch_countries()
    df_map = explode_currencies(df_countries)
    print("País x moeda (amostra):")
    print(df_map.head())
    print("Total linhas:", len(df_map))

    # 2) Série de câmbio
    end_d = date.today()
    start_d = end_d - timedelta(days=9)
    try:
        df_rates = fetch_timeseries(["BRL", "EUR", "JPY"], start_d, end_d, base="USD")
        print("\nRates (amostra):")
        print(df_rates.head())
        print("Total linhas:", len(df_rates))
    except Exception as e:
        print("\nFalha ao buscar timeseries:", e)
