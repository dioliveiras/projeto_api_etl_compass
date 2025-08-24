#!/usr/bin/env bash
set -e

# cria venv se não existir
if [ ! -d ".venv" ]; then
  python -m venv .venv
fi

# ativa venv
source .venv/bin/activate

# instala dependências
pip install -r requirements.txt

# define o PYTHONPATH para o src
export PYTHONPATH=src

# executa o pipeline
python -m etl.cli --out data/processed
