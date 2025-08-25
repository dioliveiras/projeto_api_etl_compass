# src/etl/exchangerates.py (robust fallback)
from __future__ import annotations

import os
import logging
from datetime import date
from typing import Iterable, Dict, Any, List

import httpx
import pandas as pd
from tenacity import retry, wait_exponential, stop_after_attempt, RetryError

logger = logging.getLogger("etl.exchangerates")

def _as_iso(d: date | str) -> str:
    if isinstance(d, date):
        return d.isoformat()
    return pd.to_datetime(d).date().isoformat()

def _settings():
    provider = os.getenv("EXCHANGE_PROVIDER", "").strip().lower()
    api_key = os.getenv("EXCHANGERATE_API_KEY", "").strip()
    timeout = int(os.getenv("HTTP_TIMEOUT", "30"))
    return provider, api_key, timeout

@retry(wait=wait_exponential(multiplier=1, min=1, max=8), stop=stop_after_attempt(5))
def _get_json(url: str, params: Dict[str, Any] | None = None,
              headers: Dict[str, str] | None = None, timeout: int = 30) -> Dict[str, Any]:
    with httpx.Client(timeout=timeout, headers=headers or {"User-Agent": "projeto_api_etl_compass/1.0"}) as client:
        r = client.get(url, params=params)
        r.raise_for_status()
        return r.json()

def _normalize_timeseries_payload(data: Dict[str, Any], base_fallback: str) -> pd.DataFrame:
    rates = data.get("rates")
    if not isinstance(rates, dict) or not rates:
        raise RuntimeError(f"Resposta sem 'rates' válida. Amostra: {str(data)[:300]}")

    rows: List[Dict[str, Any]] = []
    for d_str, cur_map in rates.items():
        for cur, val in (cur_map or {}).items():
            rows.append({"date": d_str, "currency_code": cur, "rate_to_base": val, "base": data.get("base", base_fallback)})

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("Timeseries retornou vazio após normalização.")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["currency_code"] = df["currency_code"].astype("string")
    df["rate_to_base"] = pd.to_numeric(df["rate_to_base"], errors="coerce").astype("float64")
    df["base"] = df["base"].astype("string")
    return df.sort_values(["currency_code", "date"]).reset_index(drop=True)

def _fetch_timeseries_apilayer(symbols: list[str], start_date: str, end_date: str,
                               base: str, timeout: int, api_key: str) -> pd.DataFrame:
    url = "https://api.apilayer.com/exchangerates_data/timeseries"
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "symbols": ",".join(symbols),
        "base": base,
        "access_key": api_key,  # alguns proxies aceitam também na query
    }
    headers = {"apikey": api_key}
    data = _get_json(url, params=params, headers=headers, timeout=timeout)
    return _normalize_timeseries_payload(data, base_fallback=base)

def _fetch_timeseries_frankfurter(symbols: list[str], start_date: str, end_date: str,
                                  base: str, timeout: int) -> pd.DataFrame:
    url = f"https://api.frankfurter.app/{start_date}..{end_date}"
    params = {"from": base, "to": ",".join(symbols)}
    data = _get_json(url, params=params, timeout=timeout)
    return _normalize_timeseries_payload(data, base_fallback=base)

def fetch_timeseries(symbols: Iterable[str], start_date: date | str, end_date: date | str,
                     base: str = "USD", timeout: int | None = None) -> pd.DataFrame:
    provider, api_key, env_timeout = _settings()
    timeout = timeout or env_timeout

    sym = [s.strip().upper() for s in symbols if s and str(s).strip()]
    if not sym:
        raise ValueError("Informe ao menos um símbolo em `symbols`.")

    start_iso, end_iso = _as_iso(start_date), _as_iso(end_date)

    # Provider APILayer primeiro (se definido), com tratamentos e fallback
    if provider in {"apilayer", "exchangeratesapi", "exchangerates_data"}:
        logger.info("Provider: APILayer (exchangerates_data)")
        if not api_key or api_key.lower() in {"sua_chave", "your_api_key_here"}:
            logger.warning("EXCHANGERATE_API_KEY ausente/inválida. Fallback para Frankfurter (base=EUR).")
            return _fetch_timeseries_frankfurter(sym, start_iso, end_iso, "EUR", timeout)

        try:
            return _fetch_timeseries_apilayer(sym, start_iso, end_iso, base, timeout, api_key)
        except RetryError as re:
            # Pode envolver HTTPStatusError 401/403; fazemos fallback direto
            logger.warning("APILayer RetryError: %s. Fallback para Frankfurter (base=EUR).", re)
            return _fetch_timeseries_frankfurter(sym, start_iso, end_iso, "EUR", timeout)
        except httpx.HTTPStatusError as he:
            code = he.response.status_code if he.response is not None else None
            if code in (401, 403):
                logger.warning("APILayer %s Unauthorized/Forbidden. Fallback para Frankfurter (base=EUR).", code)
                return _fetch_timeseries_frankfurter(sym, start_iso, end_iso, "EUR", timeout)
            # Se base for restrita pelo plano, tente EUR na APILayer primeiro
            msg = str(he).lower()
            if "base" in msg and ("restrict" in msg or "105" in msg):
                logger.warning("Restrição de base detectada. Tentando APILayer com base=EUR.")
                try:
                    return _fetch_timeseries_apilayer(sym, start_iso, end_iso, "EUR", timeout, api_key)
                except Exception as e2:
                    logger.warning("APILayer com base=EUR falhou (%s). Fallback para Frankfurter (base=EUR).", e2)
                    return _fetch_timeseries_frankfurter(sym, start_iso, end_iso, "EUR", timeout)
            raise
        except RuntimeError as rte:
            msg = str(rte).lower()
            if "base" in msg and ("restrict" in msg or "105" in msg):
                logger.warning("Restrição de base detectada. Tentando APILayer com base=EUR.")
                try:
                    return _fetch_timeseries_apilayer(sym, start_iso, end_iso, "EUR", timeout, api_key)
                except Exception as e2:
                    logger.warning("APILayer com base=EUR falhou (%s). Fallback para Frankfurter (base=EUR).", e2)
                    return _fetch_timeseries_frankfurter(sym, start_iso, end_iso, "EUR", timeout)
            logger.warning("APILayer falhou (%s). Fallback para Frankfurter (base=%s).", rte, base)
            return _fetch_timeseries_frankfurter(sym, start_iso, end_iso, base, timeout)

    # Provider padrão/grátis
    logger.info("Provider: Frankfurter (free, sem chave)")
    return _fetch_timeseries_frankfurter(sym, start_iso, end_iso, base, timeout)
