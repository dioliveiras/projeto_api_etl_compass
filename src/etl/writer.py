# src/etl/writer.py
from __future__ import annotations

from pathlib import Path
import shutil
import logging
import pandas as pd

logger = logging.getLogger("etl.writer")

def write_parquet(
    df: pd.DataFrame,
    output_dir: str | Path,
    file_stem: str = "data",
    partition_cols: list[str] | None = None,
    overwrite: bool = False,
    compression: str = "snappy",
) -> Path:
    """Grava DataFrame em Parquet.
    - Se partition_cols estiver definido, grava diretório particionado.
    - Caso contrário, grava um único arquivo <file_stem>.parquet.
    Retorna o caminho de saída (diretório ou arquivo).
    """
    out_dir = Path(output_dir)

    if overwrite and out_dir.exists():
        logger.info("Overwrite habilitado. Removendo diretório de saída: %s", out_dir)
        shutil.rmtree(out_dir, ignore_errors=True)

    out_dir.mkdir(parents=True, exist_ok=True)

    if partition_cols:
        logger.info("Gravando Parquet particionado em '%s' | partitions=%s | compression=%s",
                    out_dir, partition_cols, compression)
        df.to_parquet(
            out_dir,
            partition_cols=partition_cols,
            index=False,
            compression=compression,
            engine="pyarrow",
        )
        return out_dir
    else:
        file_path = out_dir / f"{file_stem}.parquet"
        logger.info("Gravando Parquet em '%s' | compression=%s", file_path, compression)
        df.to_parquet(
            file_path,
            index=False,
            compression=compression,
            engine="pyarrow",
        )
        return file_path

def write_csv(
    df: pd.DataFrame,
    output_path: str | Path,
    overwrite: bool = False,
    sep: str = ",",
    date_format: str = "%Y-%m-%d",
    encoding: str = "utf-8-sig",
) -> Path:
    """Grava DataFrame em CSV pronto para BI.
    - Converte colunas datetime64 para string ISO (date_format).
    - encoding 'utf-8-sig' evita problema de acentuação no Excel/Windows.
    """
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists() and not overwrite:
        raise FileExistsError(f"Arquivo já existe: {path}. Use overwrite=True.")

    df_out = df.copy()
    for col, dtype in df_out.dtypes.items():
        if "datetime64" in str(dtype):
            df_out[col] = df_out[col].dt.strftime(date_format)

    logger.info("Gravando CSV em '%s' | sep='%s'", path, sep)
    df_out.to_csv(path, index=False, sep=sep, encoding=encoding)
    logger.info("CSV gravado com sucesso.")
    return path