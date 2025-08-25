# main.py (com camada Gold: country_timeseries)
from __future__ import annotations

# --- Carrega variáveis do .env ANTES de importar módulos ---
try:
    from dotenv import load_dotenv
    load_dotenv(override=True)
except Exception:
    pass

# --- Fix de path ---
from pathlib import Path
import sys as _sys
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in _sys.path:
    _sys.path.insert(0, str(PROJECT_ROOT))

import argparse
import json
import logging
from datetime import date, timedelta
import pandas as pd

from src.etl.extractor import fetch_countries
from src.etl.writer import write_parquet, write_csv
from src.etl.transformer import transform_countries
from src.etl.exchangerates import fetch_timeseries
from src.etl.transformer_rates import transform_rates
from src.etl.transformer_enriched import enrich_countries_with_rates
from src.etl.gold import build_gold_views

logger = logging.getLogger("etl.main")

def setup_logging(level: str = "INFO") -> None:
    if logger.handlers:
        return
    lvl = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(lvl)
    ch = logging.StreamHandler(_sys.stdout)
    fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(name)s - %(message)s", "%Y-%m-%d %H:%M:%S")
    ch.setFormatter(fmt); ch.setLevel(lvl)
    logger.addHandler(ch)

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pipeline ETL: Countries + FX (bronze/silver/enriched/gold).")
    p.add_argument("--bronze-countries", default="data/bronze/countries")
    p.add_argument("--silver-countries", default="data/silver/countries")
    p.add_argument("--bronze-rates", default="data/bronze/rates")
    p.add_argument("--silver-rates", default="data/silver/rates")
    p.add_argument("--silver-enriched", default="data/silver/enriched")
    p.add_argument("--gold-dir", default="data/gold")
    p.add_argument("--base", default="USD", help="Moeda base")
    p.add_argument("--symbols", nargs="*", default=["BRL", "EUR", "USD", "JPY"], help="Moedas (ex.: BRL EUR USD JPY)")
    p.add_argument("--days", type=int, default=30, help="Janela de dias até hoje")
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--compression", default="snappy", choices=["snappy","gzip","brotli","zstd","none"])
    p.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR"])
    return p.parse_args()

# Serializer seguro p/ JSON de quality
def _json_default(o):
    from datetime import date, datetime
    import pandas as pd
    import numpy as np
    if isinstance(o, (pd.Timestamp, datetime, date)):
        return o.isoformat()
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        return float(o)
    if isinstance(o, (pd.Timedelta,)):
        return str(o)
    try:
        return o.item()
    except Exception:
        return str(o)

def _save_quality(quality: dict, name: str) -> Path:
    reports = Path("data/_reports"); reports.mkdir(parents=True, exist_ok=True)
    out = reports / f"quality_{name}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(quality, f, ensure_ascii=False, indent=2, default=_json_default)
    logger.info("Quality report salvo: %s", out)
    return out

def run_pipeline(args: argparse.Namespace) -> None:
    setup_logging(args.log_level)
    logger.info("Iniciando pipeline: Countries + FX + Gold")

    # COUNTRIES (bronze/silver)
    logger.info("Extraindo REST Countries…")
    df_countries_raw = fetch_countries()
    write_parquet(df_countries_raw, args.bronze_countries, "countries_raw",
                  partition_cols=["region"], overwrite=args.overwrite,
                  compression=None if args.compression=="none" else args.compression)

    logger.info("Transformando Countries → Silver…")
    df_countries_silver, qual_c = transform_countries(df_countries_raw)
    _save_quality(qual_c, "countries")
    write_parquet(df_countries_silver, args.silver_countries, "countries_clean",
                  partition_cols=["region"], overwrite=args.overwrite,
                  compression=None if args.compression=="none" else args.compression)

    # RATES (bronze/silver)
    end_d = date.today()
    start_d = end_d - timedelta(days=args.days)
    logger.info("Extraindo FX timeseries %s→%s | base=%s | symbols=%s", start_d, end_d, args.base, args.symbols)
    df_rates_raw = fetch_timeseries(args.symbols, start_d, end_d, base=args.base)
    write_parquet(df_rates_raw, args.bronze_rates, "fx_raw",
                  partition_cols=["date"], overwrite=args.overwrite,
                  compression=None if args.compression=="none" else args.compression)

    logger.info("Transformando Rates → Silver…")
    df_rates_silver, qual_r = transform_rates(df_rates_raw)
    _save_quality(qual_r, "rates")
    write_parquet(df_rates_silver, args.silver_rates, "fx_clean",
                  partition_cols=["date"], overwrite=args.overwrite,
                  compression=None if args.compression=="none" else args.compression)

    # ENRICHED (silver)
    logger.info("Enriquecendo (join currency_code) → Silver/Enriched…")
    df_enriched, qual_e = enrich_countries_with_rates(df_countries_silver, df_rates_silver)
    _save_quality(qual_e, "enriched")
    write_parquet(df_enriched, args.silver_enriched, "countries_fx",
                  partition_cols=["date"], overwrite=args.overwrite,
                  compression=None if args.compression=="none" else args.compression)

    # GOLD (CSVs prontos para visual)
    logger.info("Gerando camada Gold (CSVs para visual)…")
    gold_views = build_gold_views(df_enriched)
    gold_dir = Path(args.gold_dir); gold_dir.mkdir(parents=True, exist_ok=True)
    # snapshot do último dia
    write_csv(gold_views["enriched_latest"], gold_dir / "enriched_latest.csv", overwrite=args.overwrite)
    # histórico por país
    write_csv(gold_views["country_timeseries"], gold_dir / "country_timeseries.csv", overwrite=args.overwrite)
    logger.info("Gold gerado em: %s", gold_dir)

    logger.info("Pipeline finalizado com sucesso.")

def main() -> None:
    args = parse_args()
    try:
        run_pipeline(args)
    except Exception as e:
        logger.exception("Falha na execução do pipeline: %s", e)
        _sys.exit(1)

if __name__ == "__main__":
    main()