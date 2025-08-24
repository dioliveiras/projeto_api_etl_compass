import httpx
import pandas as pd

REST_COUNTRIES_URL = "https://restcountries.com/v3.1/all"

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


if __name__ == "__main__":
    df = fetch_countries()
    print(df.head())
    print(len(df))
