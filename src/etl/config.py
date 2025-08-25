# src/etl/config.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from dataclasses import dataclass

# Carrega .env da raiz do projeto (ajuste se sua estrutura for diferente)
# Procura um .env na raiz do repo (acima de src/)
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # .../projeto/ src/ etl/ config.py
ENV_FILE = PROJECT_ROOT / ".env"

# load_dotenv não dá erro se o arquivo não existir
load_dotenv(dotenv_path=ENV_FILE, override=False)


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    """Leitura centralizada de variáveis de ambiente."""
    return os.getenv(name, default)

@dataclass(frozen=True)
class Settings:
    bronze_dir: str = os.getenv("BRONZE_DIR", "data/bronze/countries")
    silver_dir: str = os.getenv("SILVER_DIR", "data/silver/countries")
    rest_countries_url: str = os.getenv("REST_COUNTRIES_URL", "https://restcountries.com/v3.1/all")
    http_timeout: int = int(os.getenv("HTTP_TIMEOUT", "30"))
    http_max_retries: int = int(os.getenv("HTTP_MAX_RETRIES", "3"))

settings = Settings()