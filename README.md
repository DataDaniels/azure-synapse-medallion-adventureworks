# 🚀 Azure Synapse Medallion Pipeline (AdventureWorks)

Este repositório contém a implementação de uma pipeline end-to-end de Engenharia de Dados desenvolvida no **Azure Synapse Analytics**, aplicando a **Medallion Architecture (Bronze, Silver e Gold)** com **Delta Lake** no **Azure Data Lake Storage Gen2 (ADLS Gen2)**.

O projeto realiza a ingestão paralela de dados a partir de um banco relacional **Azure SQL Database**, trata e padroniza as informações na camada Silver e constrói um modelo dimensional **Star Schema** na camada Gold via **PySpark**, deixando a estrutura pronta para análises e relatórios no **Power BI**.

---

## 🏗️ Arquitetura da Solução

[ Azure SQL Database ] (sqldb-adventureworks)
         │
         ▼
 ( Activity: Delete Old Data ) ──> ( ForEach_tqx: Ingestão Paralela )
                                              │
                                              ▼
                                    [ ADLS Gen2: raw/ ] (CSV)
                                              │
                                              ▼
                                 ( Notebook: Raw to Enriched )
                                              │
                                              ▼
                                    [ ADLS Gen2: enriched/ ] (Delta Lake)
                                              │
                                              ▼
                                ( Notebook: Enriched to Curated )
                                              │
                                              ▼
                                    [ ADLS Gen2: curated/ ] (Delta Lake - Star Schema)
                                              │
                                              ▼
                                     [ Power BI / Analytics ]

---

## ⚙️ Recursos Provisionados no Azure

- **Resource Group:** posgraduacao-rg
- **Synapse Workspace:** posgraduacaosynapseworkspacedaniel2026
- **Apache Spark Pool:** SparkPool01
- **Data Lake Storage (ADLS Gen2):** adlsposgraduacaodaniel
- **SQL Server:** sqlserver-pos-daniel
- **SQL Database:** sqldb-adventureworks

---

## 🗄️ Estrutura do Data Lake & Arquitetura Medalhão

### 1. Camada Raw / Bronze (`raw/`)
- **Formato:** CSV
- Contém a ingestão bruta das 10 tabelas do schema `SalesLT` (`SalesLT.Customer.csv`, `SalesLT.Product.csv`, `SalesLT.SalesOrderHeader.csv`, etc.).

### 2. Camada Enriched / Silver (`enriched/`)
- **Formato:** Delta Lake
- Processamento via notebook PySpark `Raw to Enriched`:
  - Limpeza de colunas desnecessárias e renomeação de atributos.
  - Randomização de datas na coluna `OrderDate` para simular massa histórica real.
  - Salvamento como tabelas **Delta Lake**: `salesCustomer`, `salesCustomerAddress`, `salesOrderHeader`, `salesOrderDetail`, `salesProduct`, `salesProductCategory`.

### 3. Camada Curated / Gold (`curated/`)
- **Formato:** Delta Lake (Modelo Dimensional Star Schema)
- Processamento via notebook PySpark `Enriched to Curated`:
  - Criação de *Surrogate Keys* (`monotonically_increasing_id()`).
  - Geração dinâmica da Dimensão Tempo (`dimDate`).
  - Criação das tabelas no formato **Delta Lake**:
    - **`dimCustomer`**: Dimensão Cliente tratada com endereço e chave substituta.
    - **`dimProduct`**: Dimensão Produto consolidada com categoria e subcategoria.
    - **`dimDate`**: Dimensão Calendário cobrindo o período de análises.
    - **`factSales`**: Tabela Fato de Vendas combinando cabeçalho e itens de pedidos com as chaves das dimensões.

---

## 📊 Validação e Histórico de Execução

O pipeline foi homologado com 100% de taxa de sucesso (`Succeeded`) em todas as 14 atividades no Azure Synapse Analytics:

### 1. Diagrama Visual do Pipeline
![Pipeline Flow](architecture/pipeline_flow.png)

### 2. Status e Performance de Execução das Atividades
![Pipeline Execution](architecture/pipeline_execution.png)

- **Limpeza (`Delete Old Data`):** Executada com sucesso para garantir a idempotência do pipeline.
- **Cópia Concorrente (`Copy_tqx` no `ForEach`):** Média de ~20s por tabela via Integration Runtime.
- **Job PySpark (Raw -> Enriched):** Concluído em 4m 07s.
- **Job PySpark (Enriched -> Curated):** Concluído em 3m 37s.

---

## 📁 Estrutura do Repositório

azure-synapse-medallion-adventureworks/
│
├── README.md                          # Documentação principal
│
├── architecture/
│   ├── pipeline_flow.png              # Fluxo das atividades no Synapse Studio
│   └── pipeline_execution.png         # Monitoramento da execução com status Succeeded
│
├── notebooks/
│   ├── 01_raw_to_enriched.ipynb       # Código PySpark (CSV -> Delta na Silver)
│   └── 02_enriched_to_curated.ipynb   # Código PySpark (Modelagem Star Schema na Gold)
│
└── pipelines/
    └── End_to_End_Data_Pipeline.json  # Definição do Pipeline exportada do Synapse

---

## 🛠️ Tecnologias Utilizadas

- **Microsoft Azure:** Synapse Analytics, ADLS Gen2, Azure SQL Database
- **Orquestração:** Synapse Pipelines
- **Engenharia de Dados:** Apache Spark, PySpark, Spark SQL
- **Formatos de Arquivos:** Delta Lake, CSV
- **Segurança:** Azure RBAC, Managed Identity (`Storage Blob Data Contributor`)

---

## 👨‍💻 Autor
**Daniel Moreira** | Data Engineer
