# 🚀 Azure Synapse Medallion Pipeline (AdventureWorks)

🌐 **Idioma:** [🇺🇸 English](README.md) | [🇧🇷 Português](README.pt-br.md)

[![Pipeline Canvas](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/raw/main/architecture/pipeline_flow.png)](/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/architecture/pipeline_flow.png)

Pipeline de Engenharia de Dados end-to-end no **Azure Synapse Analytics**, aplicando a **Medallion Architecture (Bronze, Silver e Gold)** com **Delta Lake** sobre **ADLS Gen2**. O projeto ingere dados relacionais de um **Azure SQL Database** (AdventureWorks LT), transforma-os via **PySpark** em um modelo dimensional **Star Schema**, e disponibiliza a camada final para consumo analítico no **Power BI**.

> 🎯 **Objetivo de negócio:** transformar dados operacionais (OLTP) de vendas e clientes em inteligência estratégica pronta para tomada de decisão — receita, ticket médio, clientes de alto valor e sazonalidade de demanda.

---

## 📖 Documentação Detalhada

Este README traz uma visão geral do projeto. Para se aprofundar:

- 🏗️ **[docs/ARCHITECTURE.pt-br.md](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/docs/ARCHITECTURE.pt-br.md)** — arquitetura completa, camadas do Data Lake, validação de execução no Synapse, modelagem Power BI, dashboard e insights de negócio.
- 💻 **[docs/CODE_WALKTHROUGH.pt-br.md](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/docs/CODE_WALKTHROUGH.pt-br.md)** — trechos comentados dos notebooks PySpark (ingestão dinâmica, surrogate keys, geração da `dimDate`, construção da fato de vendas), prontos para copiar e reutilizar.

---

## 🏗️ Arquitetura em Resumo

| Etapa      | Componente                              | Saída                    |
| ---------- | ---------------------------------------- | ------------------------ |
| 🛢️ Origem  | Azure SQL Database (`SalesLT`)          | Tabelas OLTP             |
| 🔄 Ingestão | Synapse Pipeline (`Delete` + `ForEach`) | CSV (`raw/`)             |
| 🥈 Silver   | PySpark (Spark Pool)                    | Delta Lake (`enriched/`) |
| 🥇 Gold     | PySpark / Spark SQL — Star Schema       | Delta Lake (`curated/`)  |
| 📊 Consumo  | Synapse Serverless SQL + Power BI       | Dashboards                |

**Stack:** Azure Synapse Analytics · Apache Spark Pool · ADLS Gen2 · Delta Lake · Azure SQL Database · Power BI · Azure RBAC / Managed Identity

📄 Detalhes de cada camada (Bronze/Silver/Gold) e das tecnologias em [docs/ARCHITECTURE.pt-br.md](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/docs/ARCHITECTURE.pt-br.md).


## 📊 Fonte dos Dados

Este projeto utiliza o banco de dados de exemplo **AdventureWorksLT**, disponibilizado pela Microsoft sob a licença MIT: [microsoft/sql-server-samples](https://github.com/microsoft/sql-server-samples).


[![Data Lake Containers](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/raw/main/architecture/datalake_containers.png)](/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/architecture/datalake_containers.png)

---

## 📊 Principais Resultados

Pipeline homologado com **100% de sucesso** nas 14 atividades do Synapse (Raw→Enriched em 4m07s, Enriched→Curated em 3m37s).

Extraído do dashboard executivo em Power BI:

- **Faturamento Total:** $708,7K
- **Ticket Médio:** $22,1K por pedido
- **Volume:** 2.087 unidades em 32 pedidos
- **Top categoria:** Touring Bikes ($220,7K)
- **Pico sazonal:** Outubro/2024 ($169,1K) — recorde do período analisado

📄 Análise completa de lucratividade, clientes de alto valor e sazonalidade em [docs/ARCHITECTURE.pt-br.md](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/docs/ARCHITECTURE.pt-br.md#-insights-de-negócio--respostas-analíticas).

[![Dashboard Preview](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/raw/main/dashboard/dashboard_preview.png)](/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/dashboard/dashboard_preview.png)

### 🎥 Demonstração Interativa

https://github.com/user-attachments/assets/d2af6cb1-4e54-4c98-a634-15281c31b1e8

---

### 📁 Estrutura do Repositório

```
azure-synapse-medallion-adventureworks/
│
├── LICENSE                               # Licença MIT
├── README.md                             # Visão geral do projeto (inglês)
├── README.pt-br.md                       # Visão geral do projeto (este arquivo)
│
├── docs/
│   ├── ARCHITECTURE.md
│   ├── ARCHITECTURE.pt-br.md             # Arquitetura completa, BI e insights de negócio
│   ├── CODE_WALKTHROUGH.md
│   └── CODE_WALKTHROUGH.pt-br.md         # Trechos comentados dos notebooks PySpark
│
├── architecture/
│   ├── pipeline_canvas.png               # Interface do Synapse com o pipeline em destaque
│   ├── pipeline_execution.png            # Monitoramento detalhado das execuções (Status Succeeded)
│   ├── pipeline_flow.png                 # Fluxo visual das atividades concluídas
│   ├── datalake_containers.png           # Visão geral dos 4 containers do ADLS Gen2
│   ├── datalake_raw_layer.png            # Conteúdo da camada Raw
│   ├── datalake_enriched_layer.png       # Conteúdo da camada Enriched
│   └── datalake_curated_layer.png        # Conteúdo da camada Curated
│
├── dashboard/
│   ├── ADVENTUREWORKS_DASHBOARD.pbix     # Arquivo do relatório no Power BI Desktop
│   ├── background.png                    # Plano de fundo personalizado estilo Dark Tech
│   ├── dashboard_preview.png             # Captura de tela do painel executivo final
│   ├── data_modeling.png                 # Diagrama do modelo relacional Star Schema
│   └── demo_adventureworks_dashboard.mp4 # Vídeo de demonstração interativa do dashboard
│
├── notebooks/
│   ├── 01_raw_to_enriched.ipynb          # Código PySpark (Bronze -> Silver em formato Delta)
│   └── 02_enriched_to_curated.ipynb      # Código PySpark (Silver -> Gold / Star Schema)
│
├── pipelines/
│   └── End_to_End_Data_Pipeline.json     # Definição e orquestração do pipeline no Synapse
│
└── sql/
    └── 01_create_gold_views.sql          # Script T-SQL para criação das views no Synapse Serverless
```

---

## 👨‍💻 Autor

**Daniel Moreira** | Data Engineer
