# 🚀 Azure Synapse Medallion Pipeline (AdventureWorks)

🌐 **Language:** [🇺🇸 English](README.md) | [🇧🇷 Português](README.pt-br.md)

[![Pipeline Canvas](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/raw/main/architecture/pipeline_flow.png)](/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/architecture/pipeline_flow.png)

End-to-end Data Engineering pipeline on **Azure Synapse Analytics**, applying the **Medallion Architecture (Bronze, Silver, and Gold)** with **Delta Lake** over **ADLS Gen2**. The project ingests relational data from an **Azure SQL Database** (AdventureWorks LT), transforms it via **PySpark** into a dimensional **Star Schema** model, and serves the final layer for analytical consumption in **Power BI**.

> 🎯 **Business objective:** turn operational (OLTP) sales and customer data into strategic, decision-ready intelligence — revenue, average order value, high-value customers, and demand seasonality.

---

## 📖 Detailed Documentation

This README gives an overview of the project. To go deeper:

- 🏗️ **[docs/ARCHITECTURE.md](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/docs/ARCHITECTURE.md)** — full architecture, Data Lake layers, Synapse execution validation, Power BI modeling, dashboard, and business insights.
- 💻 **[docs/CODE_WALKTHROUGH.md](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/docs/CODE_WALKTHROUGH.md)** — commented PySpark notebook snippets (dynamic ingestion, surrogate keys, `dimDate` generation, sales fact table build), ready to copy and reuse.

---

## 🏗️ Architecture Summary

| Stage       | Component                               | Output                   |
| ----------- | ---------------------------------------- | ------------------------ |
| 🛢️ Source   | Azure SQL Database (`SalesLT`)          | OLTP tables               |
| 🔄 Ingestion | Synapse Pipeline (`Delete` + `ForEach`) | CSV (`raw/`)              |
| 🥈 Silver    | PySpark (Spark Pool)                    | Delta Lake (`enriched/`)  |
| 🥇 Gold      | PySpark / Spark SQL — Star Schema       | Delta Lake (`curated/`)   |
| 📊 Consumption | Synapse Serverless SQL + Power BI     | Dashboards                |

**Stack:** Azure Synapse Analytics · Apache Spark Pool · ADLS Gen2 · Delta Lake · Azure SQL Database · Power BI · Azure RBAC / Managed Identity

📄 Details of each layer (Bronze/Silver/Gold) and the technologies used are in [docs/ARCHITECTURE.md](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/docs/ARCHITECTURE.md).

[![Data Lake Containers](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/raw/main/architecture/datalake_containers.png)](/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/architecture/datalake_containers.png)

---

## 📊 Key Results

Pipeline validated with **100% success** across all 14 Synapse activities (Raw→Enriched in 4m07s, Enriched→Curated in 3m37s).

Extracted from the Power BI executive dashboard:

- **Total Revenue:** $708.7K
- **Average Order Value:** $22.1K per order
- **Volume:** 2,087 units across 32 orders
- **Top category:** Touring Bikes ($220.7K)
- **Seasonal peak:** October/2024 ($169.1K) — record for the analyzed period

📄 Full analysis of profitability, high-value customers, and seasonality in [docs/ARCHITECTURE.md](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/docs/ARCHITECTURE.md#-business-insights--analytical-answers).

[![Dashboard Preview](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/raw/main/dashboard/dashboard_preview.png)](/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/dashboard/dashboard_preview.png)

### 🎥 Interactive Demo

**demo_adventureworks_dashboard.mp4** (see video in the `dashboard/` folder)

---

### 📁 Repository Structure

```
azure-synapse-medallion-adventureworks/
│
├── README.md                             # Project overview (English, this file)
├── README.pt-br.md                       # Project overview (Portuguese)
│
├── docs/
│   ├── ARCHITECTURE.md                   # Full architecture, BI, and business insights
│   ├── ARCHITECTURE.pt-br.md
│   ├── CODE_WALKTHROUGH.md               # Commented PySpark notebook snippets
│   └── CODE_WALKTHROUGH.pt-br.md
│
├── architecture/
│   ├── pipeline_canvas.png               # Synapse interface with the pipeline highlighted
│   ├── pipeline_execution.png            # Detailed run monitoring (Status Succeeded)
│   ├── pipeline_flow.png                 # Visual flow of completed activities
│   ├── datalake_containers.png           # Overview of the 4 ADLS Gen2 containers
│   ├── datalake_raw_layer.png            # Raw layer content
│   ├── datalake_enriched_layer.png       # Enriched layer content
│   └── datalake_curated_layer.png        # Curated layer content
│
├── dashboard/
│   ├── ADVENTUREWORKS_DASHBOARD.pbix     # Power BI Desktop report file
│   ├── background.png                    # Custom Dark Tech background
│   ├── dashboard_preview.png             # Final executive dashboard screenshot
│   ├── data_modeling.png                 # Star Schema relational model diagram
│   └── demo_adventureworks_dashboard.mp4 # Interactive dashboard demo video
│
├── notebooks/
│   ├── 01_raw_to_enriched.ipynb          # PySpark code (Bronze -> Silver, Delta format)
│   └── 02_enriched_to_curated.ipynb      # PySpark code (Silver -> Gold / Star Schema)
│
├── pipelines/
│   └── End_to_End_Data_Pipeline.json     # Pipeline definition and orchestration in Synapse
│
└── sql/
    └── 01_create_gold_views.sql          # T-SQL script to create views in Synapse Serverless
```

---

## 👨‍💻 Author

**Daniel Moreira** | Data Engineer
