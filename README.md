# Projeto ETL - API REST Countries 🌍

Pipeline simples de **ETL** (Extract, Transform, Load) para consumir dados da API [REST Countries](https://restcountries.com/), transformá-los e salvar em camadas **Bronze** e **Silver** no formato **Parquet**.

---

## 📂 Estrutura de pastas

```
projeto_api_etl_compass/
│── main.py               # Orquestra o pipeline (extract → transform → write)
│── src/
│   └── etl/
│       ├── __init__.py
│       ├── extractor.py  # Extrai dados da API
│       ├── transformer.py# Trata e padroniza dados
│       ├── writer.py     # Salva em Parquet
│
│── data/
│   ├── bronze/           # Dados crus (espelho da API)
│   └── silver/           # Dados tratados (schema padronizado)
│
│── requirements.txt
```

---

## ⚙️ Configuração do ambiente

1. Criar e ativar virtualenv:
```bash
python -m venv .venv
.venv\Scripts\activate   # Windows
source .venv/bin/activate # Linux/Mac
```

2. Instalar dependências:
```bash
pip install -r requirements.txt
```

No mínimo você precisa de:
- `pandas`
- `pyarrow`
- `httpx`

---

## ▶️ Executando o pipeline

### 1. Rodar com particionamento por `region` (padrão)
```bash
python main.py --overwrite
```
- Bronze → `data/bronze/countries/`
- Silver → `data/silver/countries/`

### 2. Rodar sem particionamento (um único arquivo parquet por camada)
```bash
python main.py --partition-cols  --overwrite
```

### 3. Personalizar diretórios de saída
```bash
python main.py --bronze-dir data/bronze/countries                --silver-dir data/silver/countries                --overwrite
```

---

## 🔎 Diferença entre Bronze e Silver

### Bronze
- Dados **brutos** da API.
- Possui nulos, tipos inconsistentes e campo `currencies` como dicionário.
- Exemplo (Brasil):
```json
{
  "country_name": "Brazil",
  "cca2": "BR",
  "cca3": "BRA",
  "region": "Americas",
  "subregion": "South America",
  "population": 203062512,
  "lat": -10.0,
  "lng": -55.0,
  "currencies": {"BRL": {"name": "Brazilian real", "symbol": "R$"}}
}
```

### Silver
- Dados **tratados e padronizados**.
- Colunas com tipos coerentes (`Int64`, `float64`, `string`).
- Nulos tratados (`region="Unknown"`).
- Moeda principal extraída para `currency_code`.
- Exemplo (Brasil):
```json
{
  "country_name": "Brazil",
  "cca2": "BR",
  "cca3": "BRA",
  "region": "Americas",
  "subregion": "South America",
  "population": 203062512,
  "lat": -10.0,
  "lng": -55.0,
  "currency_code": "BRL"
}
```

---

## 📊 Logs de execução

Exemplo de execução:

```
[2025-08-24 21:37:37] [INFO] etl.main - Iniciando pipeline: extractor → transformer → writer (bronze & silver)
[2025-08-24 21:37:37] [INFO] etl.main - Extraindo dados da API REST Countries…
[2025-08-24 21:37:38] [INFO] etl.main - Extração concluída. Linhas: 250 | Colunas: [...]
[2025-08-24 21:37:38] [INFO] etl.main - Gravando camada Bronze…
[2025-08-24 21:37:39] [INFO] etl.main - Bronze gravado em: data/bronze/countries
[2025-08-24 21:37:39] [INFO] etl.main - Transformando dados para Silver…
[2025-08-24 21:37:39] [INFO] etl.main - Transformação concluída. Linhas: 250 | Colunas: [...]
[2025-08-24 21:37:39] [INFO] etl.main - Gravando camada Silver…
[2025-08-24 21:37:39] [INFO] etl.main - Silver gravado em: data/silver/countries
```

---

## 📌 Próximos Passos

- Criar camada **Gold** (métricas e KPIs para análise).
- Conectar no **Power BI** para dashboards.
- Adicionar testes unitários.
- Usar orquestrador (Airflow/Prefect) em ambiente de produção.
