# Projeto API ETL Compass

Pipeline de **Extração, Transformação e Carga (ETL)** desenvolvido em Python para consolidar informações de países e suas cotações cambiais em relação ao Euro.  
Este projeto faz parte da disciplina de **Linguagens de Programação** (Engenharia de Dados - UNIFOR).

---

## 🚀 Objetivo
- Extrair dados da API **REST Countries** e da **Exchange Rates API**.  
- Transformar e enriquecer os dados (normalização, junção e agregações).  
- Carregar os resultados em camadas de dados (`bronze`, `silver`, `gold`).  
- Disponibilizar relatórios para análise em ferramentas de BI (ex.: Power BI).

---

## 📂 Estrutura do Projeto

```
projeto_api_etl_compass/
│── src/              # Código ETL organizado em pacotes
│── data/             # Estrutura de dados (bronze, silver, gold, _reports)
│── run.bat           # Execução rápida (Windows)
│── run.sh            # Execução rápida (Linux/Mac)
│── Dockerfile        # (opcional) Build em container
│── requirements.txt  # Dependências do projeto
│── README.md         # Este arquivo :)
```

> 🔹 **Observação:** Arquivos de dados (`.csv`, `.parquet`, etc.) **não são versionados**. Apenas a estrutura de pastas é mantida usando  `.gitkeep`.

---

## ⚙️ Como Executar

### Windows
```powershell
.
un.bat
```

### Linux / Mac
```bash
chmod +x run.sh
./run.sh
```

### Docker (opcional)
```bash
docker build -t projeto_api_etl .
docker run --rm -v $(pwd)/data:/app/data projeto_api_etl
```

---

## 📊 Visualização no Power BI

O resultado do ETL foi analisado no Power BI.  
Painel construído: **Comparativo Cambial por País em relação ao Euro**.

![Dashboard Power BI](./docs/dashboard.png)

> O arquivo `dashboard.png` foi exportado a partir do Power BI e está em `docs/`.

---

## ✅ Requisitos Atendidos
- Modularização do código.
- Orientação a objetos.
- Tratamento de exceções.
- Uso de decorators e padrões de projeto.
- Registro de logs.
- Estrutura de pacotes Python.
- Camadas de dados (`bronze`, `silver`, `gold`).
- Visualização final dos dados no Power BI.

---

## 👨‍💻 Autor
Anderson Oliveira – [GitHub](https://github.com/dioliveiras)
