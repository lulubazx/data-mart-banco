# End-to-End Bank Data Pipeline

Pipeline de Engenharia de Dados completo simulando um ambiente financeiro real, cobrindo todo o fluxo desde um banco transacional até a camada de consumo analítica.

O projeto vai de um PostgreSQL operacional até um dashboard executivo, passando por Data Lake no BigQuery, transformações com dbt, orquestração com Airflow e exposição dos dados por meio de uma API em Ruby.

---

## Visão Geral da Arquitetura

```mermaid
graph TD
    A[PostgreSQL Transacional] -->|Extract & Load| B[Python Ingestion]
    B --> C[BigQuery - Raw Layer]
    C -->|dbt Staging| D[BigQuery - Staging]
    D -->|dbt Marts| E[BigQuery - Data Marts]
    E -->|KPIs| F[Tabelas Analíticas]
    F --> G[Ruby API]

    H[Apache Airflow] --> B
    H --> D
    H --> E
```

### Diagrama Detalhado (Segurança e Camadas)

```mermaid
graph LR
  subgraph Origem
    PG[(PostgreSQL)]
  end
  subgraph Ingestao
    ING[Ingestion Service]
    LOGS[(Logs/Metrics)]
  end
  subgraph BigQuery
    RAW[bank_raw]
    STG[bank_raw_staging]
    MART[bank_mart]
    META[bank_meta]
  end
  subgraph Consumo
    API[Ruby API]
  end
  PG --> ING --> STG --> RAW
  RAW --> MART
  ING --> LOGS
  ING --> META
  MART --> API
```

---

## Tech Stack

* Fonte: PostgreSQL (Docker)
* Ingestão: Python (Pandas, SQLAlchemy)
* Data Warehouse: Google BigQuery
* Transformação: dbt (Data Build Tool)
* Orquestração: Apache Airflow (Docker)
* Consumo: Ruby (simulação de backend API)
* Infraestrutura: Docker e Docker Compose

---

## Arquitetura de Dados

1. Ingestão (Extract & Load)

   * Scripts em Python extraem dados de clientes, transações e empréstimos do PostgreSQL.
   * Os dados brutos são carregados no BigQuery na camada `bank_raw`.

2. Transformação (Transform)

   * O dbt é responsável pela modelagem analítica:

     * Staging: limpeza, padronização e tipagem dos dados.
     * Marts: construção de fatos e dimensões utilizando Star Schema.
     * KPIs: cálculo de métricas como saldo líquido, exposição ao risco e segmentação de clientes.

3. Orquestração

   * Uma DAG no Airflow executa diariamente o pipeline completo.
   * Dependências garantem a ordem correta: ingestão, testes de qualidade e transformações dbt.

4. Serving

   * Uma aplicação em Ruby consome os dados tratados diretamente do BigQuery.
   * A API simula o backend de um aplicativo bancário ou dashboard executivo.

---

## Como Executar o Projeto

### Pré-requisitos

* Docker e Docker Compose
* Conta no Google Cloud Platform com Service Account
* Python 3.8 ou superior

---

### Configuração do Ambiente

1. Clone o repositório.
2. Copie `.env.example` para `.env` e preencha com valores reais.
3. Adicione a credencial de Service Account do GCP em `secrets/gcp-sa.json` (somente para uso local).
4. Configure o SDK do Google Cloud localmente ou use `GOOGLE_APPLICATION_CREDENTIALS` via `.env`.
5. Gere `AIRFLOW_FERNET_KEY` (ex.: `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`).
6. Ajuste `config/tables.yaml` e `config/schemas.yaml` conforme seu esquema real.

---

### Infraestrutura (Airflow)

```bash
cd airflow_infra
docker-compose --env-file ../.env up -d --build
```

A interface do Airflow ficará disponível em:

```
http://localhost:8085
```

Credenciais de acesso:

* Definidas em `.env` (AIRFLOW_ADMIN_USER / AIRFLOW_ADMIN_PASSWORD)

---

### Execução do Pipeline

1. Acesse o Airflow.
2. Ative a DAG `bank_data_pipeline`.
3. O pipeline executará automaticamente a ingestão e as transformações dbt.

---

### Teste da Aplicação Ruby

Para simular o consumo dos dados analíticos:

```bash
cd ruby_app
docker build -t banco-ruby-api .
docker run --env-file ..\.env -v ${PWD}/..\secrets:/app/secrets:ro banco-ruby-api
```

---

## Resultados

O pipeline gera a tabela analítica `kpi_performance_geral`, permitindo segmentação de clientes e análise financeira em tempo quase real.

Exemplo de resposta da API Ruby:

```json
{ "nome": "Maria S.", "saldo": 15000.0, "categoria": "Varejo" }
{ "nome": "João S.", "saldo": -3500.0, "categoria": "Varejo" }
```

---

## Decisões Técnicas

- Ingestão com staging + MERGE no BigQuery para idempotência.
- Configurações em YAML para tabelas, colunas de data e validações.
- Observabilidade via logs estruturados e métricas em `bank_meta`.
- Qualidade de dados com testes dbt e testes SQL custom.
- Segredos: suporte a Airflow Connections via `POSTGRES_CONN_ID`.

---

### Evidências de Execução

### Pipeline Executado com Sucesso
Fluxo de orquestração completo no Airflow: da ingestão PostgreSQL à modelagem dbt
![Airflow Graph](docs/airflow_success.PNG)

### Saída da API
Aplicação Ruby a consumir dados tratados do BigQuery via Google Client Library
![Ruby Output](docs/ruby_terminal.PNG)

### BigQuery Preview
Data Mart final no BigQuery pronto para consumo analítico
![BigQuery View](docs/bigquery.PNG)

---

## Runbook (Troubleshooting)

- Airflow Webserver não abre: verifique `AIRFLOW__WEBSERVER__WEB_SERVER_MASTER_TIMEOUT` e os logs do container.
- Erros de ingestão no Airflow: valide conexão Postgres e variáveis `POSTGRES_*`.
- dbt falha por credenciais: confirme `BIGQUERY_PROJECT_ID` e o tipo de credencial (service account em produção).
- dbt_utils: rode `dbt deps` antes de `dbt test`.
- BigQuery sem billing: MERGE será substituído por CREATE OR REPLACE automaticamente.

---

## Data Dictionary (Resumo)

- `stg_users`: clientes (user_id, nome_completo, email, data_cadastro)
- `stg_loans`: empréstimos (loan_id, user_id, valor_emprestimo, status, taxa_juros, data_solicitacao)
- `stg_investments`: investimentos (investment_id, user_id, tipo_investimento, valor_investido, data_investimento)
- `stg_cards`: transações de cartão (transaction_id, user_id, valor_transacao, categoria, data_transacao)

---

## Data Lineage

- Gere a documentação: `dbt docs generate`
- Sirva localmente: `dbt docs serve`
- Integração recomendada: DataHub ou Amundsen (catálogo e lineage).

---

## Autor

Ricardo
Data Engineer
