from __future__ import annotations

from datetime import date, timedelta
import os
import time
from typing import Iterable, List
import httpx
import pandas as pd

from .config import get_env 

# ---------------- Endpoints
REST_COUNTRIES_URL = "https://restcountries.com/v3.1/all"
EXCHANGERATE_TS_URL = "https://api.exchangerate.host/timeseries"

# ============== REST Countries ==============
def fetch_countries() -> pd.DataFrame:
    """
    Busca lista de países da API REST Countries e retorna como DataFrame.
    Campos principais: nome, siglas, região, sub-região, moedas.
    """
    params = {"fields": "name,cca2,cca3,currencies,region,subregion,population,latlng"}
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


def _need_api_key_hint(err_obj: dict | None) -> bool:
    msg = (err_obj or {}).get("type") or (err_obj or {}).get("message") or ""
    msg = str(msg).lower()
    return "access_key" in msg or "api key" in msg or "apikey" in msg


def fetch_timeseries(
    symbols: list[str],
    start_d: date,
    end_d: date,
    base: str = "USD",
    api_key: str | None = None,
    max_batch: int = 20,
    timeout_s: int = 60,
    retries: int = 3,
    backoff_s: float = 1.5,
) -> pd.DataFrame:
    """
    Busca série histórica de câmbio em lotes (até 20 símbolos por vez).
    Retorna colunas: date (str), currency_code, rate_to_usd (float).

    - Usa chave de API se disponível (param 'access_key' ou env EXCHANGERATE_API_KEY).
    - Faz retries exponenciais para 429/5xx/timeout.
    - Valida janela máxima de 365 dias (limite do provedor).
    """
    # saneamento de entrada
    symbols = sorted(set([s.upper() for s in symbols if isinstance(s, str) and len(s) == 3]))
    if not symbols or start_d > end_d:
        return pd.DataFrame(columns=["date", "currency_code", "rate_to_usd"])

    # limite de janela
    if (end_d - start_d).days > 365:
        raise ValueError("Janela máxima para timeseries é 365 dias.")

    # api key (opcional)
    api_key = 'api_key or os.getenv("EXCHANGERATE_API_KEY") or None'

    frames: list[pd.DataFrame] = []
    for batch in _chunk(symbols, max_batch):
        params = {
            "base": base.upper(),
            "symbols": ",".join(batch),
            "start_date": start_d.isoformat(),
            "end_date": end_d.isoformat(),
        }
        if api_key:
            params["access_key"] = api_key

        attempt = 0
        while True:
            attempt += 1
            try:
                resp = httpx.get(EXCHANGERATE_TS_URL, params=params, timeout=timeout_s)
                # Retry em 429/5xx
                if resp.status_code in (429, 500, 502, 503, 504):
                    if attempt <= retries:
                        time.sleep(backoff_s ** attempt)
                        continue
                    resp.raise_for_status()
                resp.raise_for_status()
                data = resp.json()

                # Alguns retornos incluem 'success'
                if isinstance(data, dict) and data.get("success") is False:
                    err = data.get("error") or {}
                    # mensagem amigável para falta de access_key
                    if _need_api_key_hint(err):
                        raise RuntimeError(
                            "exchangerate.host exigiu chave de API. "
                            "Defina EXCHANGERATE_API_KEY no ambiente ou passe api_key=..."
                        )
                    raise RuntimeError(f"exchangerate.host retornou erro: {err}")

                rates = (data or {}).get("rates")
                if not isinstance(rates, dict) or not rates:
                    # log breve para depuração
                    sample = str(data)[:300]
                    raise RuntimeError(f"Resposta sem 'rates' válida. Amostra: {sample}")

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
                break  # saiu do loop de retry após sucesso

            except (httpx.ReadTimeout, httpx.ConnectTimeout):
                if attempt <= retries:
                    time.sleep(backoff_s ** attempt)
                    continue
                raise
            except httpx.HTTPError:
                # erros não-transientes
                raise

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
