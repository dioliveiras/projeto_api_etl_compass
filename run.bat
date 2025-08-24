@echo off
REM cria a venv (se não existir)
if not exist .venv (
    python -m venv .venv
)

REM ativa a venv
call .venv\Scripts\activate

REM instala dependências
pip install -r requirements.txt

REM define o PYTHONPATH para o src
set PYTHONPATH=src

REM executa o pipeline
python -m etl.cli --out data\processed
