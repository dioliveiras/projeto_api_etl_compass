# main.py
from __future__ import annotations

# --- Fix de path: garante que a raiz do projeto esteja no sys.path ---
from pathlib import Path
import sys as _sys

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in _sys.path:
    _sys.path.insert(0, str(PROJECT_ROOT))
# ---------------------------------------------------------------------

import argparse
import logging
import pandas as pd

from src.etl.extractor import fetch_countries
from src.etl.transformer import transform_countries
from src.etl.writer import write_parquet

logger = logging.getLogger("etl.main")


def setup_logging(level: str = "INFO") -> None:
    if logger.handlers:
        return
    lvl = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(lvl)
    ch = logging.StreamHandler(_sys.stdout)
    fmt = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch.setFormatter(fmt)
    ch.setLevel(lvl)
    logger.addHandler(ch)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Pipeline ETL: extrai países da REST Countries e grava camadas Bronze e Silver em Parquet."
    )
    # Saídas (você pode customizar via CLI)
    p.add_argument("--bronze-dir", default="data/bronze/countries", help="Diretório de saída Bronze.")
    p.add_argument("--silver-dir", default="data/silver/countries", help="Diretório de saída Silver.")

    # Config de escrita
    p.add_argument("--partition-cols", nargs="*", default=["region"],
                   help="Colunas para particionar (ex.: region subregion). Deixe vazio para arquivo único.")
    p.add_argument("--overwrite", action="store_true", help="Sobrescreve a saída existente.")
    p.add_argument("--compression", default="snappy",
                   choices=["snappy", "gzip", "brotli", "zstd", "none"], help="Codec de compressão do Parquet.")

    # Logs
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Nível de log.")
    return p.parse_args()


def run_pipeline(
    bronze_dir: str | Path,
    silver_dir: str | Path,
    partition_cols: list[str] | None,
    overwrite: bool,
    compression: str,
) -> tuple[Path, Path]:
    logger.info("Iniciando pipeline: extractor → transformer → writer (bronze & silver)")

    # 1) Extract
    logger.info("Extraindo dados da API REST Countries…")
    df_raw: pd.DataFrame = fetch_countries()
    logger.info("Extração concluída. Linhas: %s | Colunas: %s", len(df_raw), list(df_raw.columns))

    # 2) Bronze (dados brutos)
    logger.info("Gravando camada Bronze…")
    compression_arg = None if compression == "none" else compression
    bronze_path = write_parquet(
        df=df_raw,
        output_dir=bronze_dir,
        file_stem="countries_raw",
        partition_cols=partition_cols if partition_cols else None,
        overwrite=overwrite,
        compression=compression_arg or "snappy",
    )
    logger.info("Bronze gravado em: %s", bronze_path)

    # 3) Transform
    logger.info("Transformando dados para Silver…")
    df_silver, quality = transform_countries(df_raw)
    logger.info("Transformação concluída. Linhas: %s | Colunas: %s", len(df_silver), list(df_silver.columns))

    # Relatório de qualidade (resumo)
    logger.info("Quality (linhas): %s", quality.get("rows"))
    logger.info("Quality (nulos por coluna): %s", quality.get("nulls_per_column"))
    logger.info("Quality (duplicados por cca3): %s", quality.get("duplicate_cca3"))
    logger.debug("Quality (amostra): %s", quality.get("sample"))

    # 4) Silver (dados tratados)
    logger.info("Gravando camada Silver…")
    silver_path = write_parquet(
        df=df_silver,
        output_dir=silver_dir,
        file_stem="countries_clean",
        partition_cols=partition_cols if partition_cols else None,
        overwrite=overwrite,
        compression=compression_arg or "snappy",
    )
    logger.info("Silver gravado em: %s", silver_path)

    logger.info("Pipeline finalizado com sucesso. Bronze: %s | Silver: %s", bronze_path, silver_path)
    return Path(bronze_path), Path(silver_path)


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)
    try:
        run_pipeline(
            bronze_dir=args.bronze_dir,
            silver_dir=args.silver_dir,
            partition_cols=args.partition_cols if args.partition_cols else None,
            overwrite=args.overwrite,
            compression=args.compression,
        )
    except Exception as e:
        logger.exception("Falha na execução do pipeline: %s", e)
        _sys.exit(1)


if __name__ == "__main__":
    main()
