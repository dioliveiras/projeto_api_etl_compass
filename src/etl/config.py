# src/etl/config.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Carrega .env da raiz do projeto (ajuste se sua estrutura for diferente)
# Procura um .env na raiz do repo (acima de src/)
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # .../projeto/ src/ etl/ config.py
ENV_FILE = PROJECT_ROOT / ".env"

# load_dotenv não dá erro se o arquivo não existir
load_dotenv(dotenv_path=ENV_FILE, override=False)


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    """Leitura centralizada de variáveis de ambiente."""
    return os.getenv(name, default)
