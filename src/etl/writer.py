"""
writer.py — Módulo de escrita para salvar DataFrames em Parquet.

Recursos:
- Salva DataFrame em Parquet (engine: pyarrow, compressão: snappy)
- Opção de particionamento por colunas (partition_cols)
- Modo overwrite (remove destino antes de escrever)
- Logs estruturados e tratamento de exceções
- Sanitização de nomes de colunas
- (Opcional) coerção de tipos por schema

Dependências:
- pandas
- pyarrow

Exemplo de uso:
    from writer import write_parquet

    write_parquet(
        df=countries_df,
        output_dir="data/bronze/countries",
        file_stem="countries",
        partition_cols=["region"],
        overwrite=True,
    )
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd

# ======================
# Configuração de logger
# ======================
logger = logging.getLogger("etl.writer")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    _ch = logging.StreamHandler()
    _fmt = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _ch.setFormatter(_fmt)
    logger.addHandler(_ch)


# ======================
# Utilidades
# ======================
def _ensure_dataframe(df: pd.DataFrame) -> None:
    if df is None:
        raise ValueError("DataFrame é None.")
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Objeto recebido não é um pandas.DataFrame.")
    if df.empty:
        raise ValueError("DataFrame está vazio; nada para gravar.")


def _sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza nomes de colunas para evitar problemas no filesystem / leitores:
    - strip espaços
    - substitui espaços por underscore
    - remove barras e pontuações problemáticas
    - força minúsculas
    """
    def _san(s: str) -> str:
        s = s.strip().replace(" ", "_").replace("/", "_").replace("\\", "_")
        s = s.replace("(", "").replace(")", "").replace("[", "").replace("]", "")
        s = s.replace("{", "").replace("}", "").replace(":", "_").replace(";", "_")
        s = s.replace(",", "_").replace(".", "_")
        return s.lower()

    df = df.copy()
    df.columns = [_san(str(c)) for c in df.columns]
    return df


def _cast_schema(df: pd.DataFrame, schema: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """
    Converte tipos de colunas conforme mapeamento {col: dtype}, quando fornecido.
    dtype segue convenções pandas/pyarrow compatíveis (ex.: "string", "Int64", "float64", "datetime64[ns]").
    Ignora colunas não presentes no df.
    """
    if not schema:
        return df

    df = df.copy()
    for col, dtype in schema.items():
        if col not in df.columns:
            logger.warning(f"Schema: coluna '{col}' não está no DataFrame; ignorando coerção.")
            continue
        try:
            if dtype.startswith("datetime64"):
                df[col] = pd.to_datetime(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(dtype)
        except Exception as e:
            logger.error(f"Falha ao converter coluna '{col}' para '{dtype}': {e}")
            raise
    return df


def _remove_if_exists(path: Path) -> None:
    if path.exists():
        if path.is_file():
            path.unlink()
        else:
            shutil.rmtree(path)


# ======================
# Escrita Parquet
# ======================
def write_parquet(
    df: pd.DataFrame,
    output_dir: str | Path,
    file_stem: str = "dataset",
    partition_cols: Optional[Iterable[str]] = None,
    overwrite: bool = True,
    compression: str = "snappy",
    schema: Optional[Dict[str, str]] = None,
) -> Path:
    """
    Escreve o DataFrame em Parquet.

    Parâmetros
    ----------
    df : pd.DataFrame
        DataFrame a ser gravado.
    output_dir : str | Path
        Diretório base de saída (será criado se não existir).
        - Se NÃO houver particionamento, o arquivo final será: {output_dir}/{file_stem}.parquet
        - Se houver particionamento, será um diretório com subpastas por partição.
    file_stem : str
        Nome base do arquivo (sem extensão) quando não há particionamento.
    partition_cols : Iterable[str] | None
        Colunas para particionar (cria layout de diretórios estilo Hive).
    overwrite : bool
        Se True, remove o destino antes de escrever.
    compression : str
        Compressão do Parquet (ex.: 'snappy', 'gzip', 'brotli', 'zstd').
    schema : Dict[str, str] | None
        (Opcional) Mapeamento de colunas para dtypes antes da escrita.

    Retorna
    -------
    Path
        Caminho do arquivo (sem partição) ou diretório raiz (com partição).
    """
    _ensure_dataframe(df)
    df = _sanitize_columns(df)
    df = _cast_schema(df, schema)

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    engine = "pyarrow"

    if partition_cols:
        # Escrita particionada (gera pasta base; pandas cria subpastas por partição)
        target_path = output_dir
        if overwrite:
            logger.info(f"Overwrite habilitado. Removendo diretório de saída: {target_path}")
            _remove_if_exists(target_path)

        logger.info(
            f"Gravando Parquet particionado em '{target_path}' | "
            f"partitions={list(partition_cols)} | compression={compression}"
        )

        try:
            df.to_parquet(
                target_path,
                engine=engine,
                compression=compression,
                partition_cols=list(partition_cols),
                index=False,
            )
        except Exception as e:
            logger.exception(f"Erro ao gravar Parquet particionado: {e}")
            raise
        else:
            logger.info("Gravação concluída com sucesso.")
        return target_path
    else:
        # Arquivo único
        target_path = output_dir / f"{file_stem}.parquet"
        if overwrite:
            logger.info(f"Overwrite habilitado. Removendo destino caso exista: {target_path}")
            _remove_if_exists(target_path)

        logger.info(
            f"Gravando Parquet em '{target_path}' | compression={compression}"
        )

        try:
            df.to_parquet(
                target_path,
                engine=engine,
                compression=compression,
                index=False,
            )
        except Exception as e:
            logger.exception(f"Erro ao gravar Parquet: {e}")
            raise
        else:
            logger.info("Gravação concluída com sucesso.")
        return target_path


# ======================
# Execução direta (opcional)
# ======================
if __name__ == "__main__":
    # Exemplo mínimo: cria um DF dummy e grava (útil para testar rapidamente o módulo isolado).
    data = {
        "country_name": ["Brazil", "France"],
        "cca2": ["BR", "FR"],
        "region": ["Americas", "Europe"],
        "population": [203_000_000, 68_000_000],
    }
    df_demo = pd.DataFrame(data)
    out = write_parquet(
        df=df_demo,
        output_dir="data/output_demo",
        file_stem="countries_demo",
        partition_cols=None,  # ou ["region"]
        overwrite=True,
        compression="snappy",
    )
    logger.info(f"Arquivo gerado em: {out}")
