# src/etl/extractors.py
from __future__ import annotations

from datetime import date, timedelta
import httpx
import pandas as pd

# ---------------- Endpoints
REST_COUNTRIES_URL = "https://restcountries.com/v3.1/all"
EXCHANGERATE_TS_URL = "https://api.exchangerate.host/timeseries"


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
def _chunk(lst: list[str], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def fetch_timeseries(
    symbols: list[str],
    start_d: date,
    end_d: date,
    base: str = "USD",
) -> pd.DataFrame:
    """
    Busca série histórica de câmbio em lotes (20 símbolos por vez).
    Retorna colunas: date (str), currency_code, rate_to_usd (float).
    Levanta erro com detalhe se a API responder 'success': False.
    """
    # saneamento de entrada
    symbols = sorted(set([s.upper() for s in symbols if isinstance(s, str) and len(s) == 3]))
    if not symbols or start_d > end_d:
        return pd.DataFrame(columns=["date", "currency_code", "rate_to_usd"])

    frames = []
    for batch in _chunk(symbols, 20):
        params = {
            "base": base.upper(),
            "symbols": ",".join(batch),
            "start_date": start_d.isoformat(),
            "end_date": end_d.isoformat(),
        }
        resp = httpx.get(EXCHANGERATE_TS_URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        # alguns retornos possuem a chave 'success'
        if isinstance(data, dict) and data.get("success") is False:
            err = data.get("error") or {}
            raise RuntimeError(f"exchangerate.host retornou erro: {err}")

        rates = (data or {}).get("rates") or {}
        rows = []
        for d_str, mapping in rates.items():
            for code, val in (mapping or {}).items():
                if val is not None:
                    rows.append({
                        "date": d_str,
                        "currency_code": code,
                        "rate_to_usd": float(val),
                    })
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

    # 2) Rates de um período curto para 3 moedas conhecidas
    end_d = date.today()
    start_d = end_d - timedelta(days=9)  # últimos ~10 dias
    try:
        df_rates = fetch_timeseries(["BRL", "EUR", "JPY"], start_d, end_d, base="USD")
        print("\nRates (amostra):")
        print(df_rates.head())
        print("Total linhas:", len(df_rates))
    except Exception as e:
        print("\nFalha ao buscar timeseries:", e)
