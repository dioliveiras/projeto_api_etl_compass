@echo off
setlocal
cd /d %~dp0

REM cria a venv (se não existir)
if not exist .venv\Scripts\python.exe (
  python -m venv .venv
)

REM ativa a venv
call .venv\Scripts\activate

REM instala dependências
python -m pip install --upgrade pip
pip install -r requirements.txt

REM (opcional) sobrescrever provider pelo ambiente; o .env já é carregado pelo main.py
REM set EXCHANGE_PROVIDER=frankfurter

REM executa o pipeline (padrão) + passa quaisquer args extras
python .\main.py --overwrite --days 30 --symbols BRL EUR USD JPY %*

endlocal
