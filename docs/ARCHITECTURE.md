# 🏗️ Architecture, BI & Business Insights

🌐 **Language:** [🇺🇸 English](ARCHITECTURE.md) | [🇧🇷 Português](ARCHITECTURE.pt-br.md)

> ⬅️ Back to the [main README](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/README.md) · See also the [Code Walkthrough (PySpark)](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/docs/CODE_WALKTHROUGH.md)

This document details the business context, the full solution architecture, pipeline validation in Azure Synapse, the Power BI presentation layer, and the analytical insights extracted from the **AdventureWorks Medallion Pipeline** project.

---

## 🎯 Business Context & Analytical Objectives

More than a robust cloud technical solution, this project was designed to solve a classic business challenge: **turning operational (OLTP) sales and customer data into strategic, decision-ready intelligence.**

The processed dataset belongs to the **AdventureWorks** ecosystem (a global bicycle, parts, and accessories manufacturer/distributor). The dimensional structure built in the Gold (`curated`) layer directly answers the following strategic questions:

- **Commercial Performance & Profitability:**
  * What is the Total Revenue ($), Average Order Value, and Sales Volume per period?
  * Which products and categories are the most profitable and highest-turnover?
- **Customer Intelligence:**
  * Who are the high-value customers?
- **Time & Seasonality Analysis:**
  * What is the historical sales trend over months and years (supported by `dimDate`)?
  * Are there seasonal demand peaks for specific product categories?

---

## 🏗️ Solution Architecture

The data flow was built following an end-to-end cloud pipeline pattern:

| Stage             | Main Component              | Operation Description                                                             | Format / Output              |
| ------------------ | ---------------------------- | ------------------------------------------------------------------------------------ | ----------------------------- |
| **🛢️ Source**      | **Azure SQL Database**       | Relational data source (`SalesLT` schema).                                          | OLTP tables                   |
| **🔄 Ingestion**    | **Synapse Pipeline**         | `Delete` activity for idempotency and `ForEach` for parallel copy.                   | CSV files (`raw/`)            |
| **🥈 Silver**       | **PySpark** *(Spark Pool)*   | Schema cleanup, column handling, and historical date simulation.                     | Delta tables (`enriched/`)    |
| **🥇 Gold**         | **PySpark / Spark SQL**      | Star Schema modeling with Surrogate Keys and Calendar Dimension.                     | Delta tables (`curated/`)     |
| **📊 Consumption**  | **Power BI / Analytics**     | Final layer ready for analytical modeling and dashboards.                            | Dashboards & Reports          |

---

## ⚙️ Technologies & Azure Services Used

- **Azure Synapse Analytics:** Integrated environment for pipeline orchestration and Big Data processing.
- **Apache Spark Pool:** Managed cluster for running PySpark transformation jobs.
- **Azure Data Lake Storage Gen2 (ADLS Gen2):** Cloud storage optimized for analytics with hierarchical namespace.
- **Delta Lake:** Open-source storage format bringing ACID transactions and high performance to the Data Lake.
- **Azure SQL Database:** Source relational database (AdventureWorks LT test base).
- **Azure RBAC & Managed Identity:** Unified and secure access control between services without exposing keys.

---

## 🗄️ Data Lake Structure & Medallion Architecture

[![Data Lake Containers](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/raw/main/architecture/datalake_containers.png)](/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/architecture/datalake_containers.png)

The Storage Account (ADLS Gen2) was organized into three main containers, following the Raw → Enriched → Curated flow:

### 1. Raw / Bronze Layer (`raw/`)

- **Format:** CSV
- Contains the raw ingestion of the 10 tables from the `SalesLT` schema (`SalesLT.Customer.csv`, `SalesLT.Product.csv`, `SalesLT.SalesOrderHeader.csv`, etc.).

[![Raw Layer](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/raw/main/architecture/datalake_raw_layer.png)](/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/architecture/datalake_raw_layer.png)

### 2. Enriched / Silver Layer (`enriched/`)

- **Format:** Delta Lake
- Processed via the `01_raw_to_enriched` PySpark notebook:
  * Cleanup of unnecessary columns and attribute renaming.
  * Randomization of dates in the `OrderDate` column to simulate a realistic historical dataset.
  * Saved as **Delta Lake** tables: `salesCustomer`, `salesCustomerAddress`, `salesOrderHeader`, `salesOrderDetail`, `salesProduct`, `salesProductCategory`.

[![Enriched Layer](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/raw/main/architecture/datalake_enriched_layer.png)](/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/architecture/datalake_enriched_layer.png)

### 3. Curated / Gold Layer (`curated/`)

- **Format:** Delta Lake (Star Schema dimensional model)
- Processed via the `02_enriched_to_curated` PySpark notebook:
  * Creation of Surrogate Keys (`monotonically_increasing_id()`).
  * Dynamic generation of the Time Dimension (`dimDate`).
  * Creation of the following **Delta Lake** tables:
    + **`dimCustomer`**: Cleaned Customer Dimension with address and surrogate key.
    + **`dimProduct`**: Consolidated Product Dimension with category and subcategory.
    + **`dimDate`**: Calendar Dimension covering the analysis period.
    + **`factSales`**: Sales Fact table combining order headers and line items with dimension keys.

[![Curated Layer](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/raw/main/architecture/datalake_curated_layer.png)](/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/architecture/datalake_curated_layer.png)

> 🔍 Want to see the commented PySpark code behind each of these steps? See the [Code Walkthrough](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/docs/CODE_WALKTHROUGH.md).

---

## 📊 Execution Validation & History

The pipeline was validated with a 100% success rate (`Succeeded`) across all 14 activities in Azure Synapse Analytics:

### Activity Flow & Execution

[![Pipeline Flow](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/raw/main/architecture/pipeline_canvas.png)](/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/architecture/pipeline_canvas.png)

### Detailed Monitoring & Performance

[![Pipeline Execution](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/raw/main/architecture/pipeline_execution.png)](/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/architecture/pipeline_execution.png)

- **Cleanup (`Delete Old Data`):** Successfully executed to ensure pipeline idempotency.
- **Concurrent Copy (`Copy_tqx` in `ForEach`):** ~20s average per table via Integration Runtime.
- **PySpark Job (Raw -> Enriched):** Completed in 4m 07s.
- **PySpark Job (Enriched -> Curated):** Completed in 3m 37s.

---

## 📊 Presentation Layer & Business Intelligence (Power BI)

The Gold (`curated`) Data Lake layer was made available via **Synapse Serverless SQL Pool** and connected to **Power BI Desktop** to build an interactive executive sales dashboard.

### 📐 Dimensional Modeling (Star Schema)

In Power BI Desktop, the Synapse views were connected and organized into a **Star Schema (one-to-many)** structure, ensuring relational integrity, high analytical performance, and efficient DAX measure calculation:

[![Data Modeling](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/raw/main/dashboard/data_modeling.png)](/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/dashboard/data_modeling.png)

- **Fact Table (`factSales`):** Centralized with financial metrics and surrogate keys.
- **Dimension Tables (`dimCustomer`, `dimProduct`, `dimDate`):** Contextual entities linked to the fact table via relational keys (`customerKey`, `productKey`, `dateKey`).

### 🛠️ SQL Abstraction Layer (Synapse Serverless SQL Pool)

To connect **Power BI Desktop** to the Delta Lake files persisted in the Gold layer of the Data Lake (ADLS Gen2), the `sql/01_create_gold_views.sql` script is used.

This script is fundamental to the architecture for the following technical reasons:

- **Bridge between Files and T-SQL:** Power BI consumes data efficiently via relational SQL queries. Using `OPENROWSET` with `FORMAT = 'DELTA'` lets the Serverless engine read parquet/delta files directly from the file repository and expose them as native database tables.
- **Abstraction Layer (Decoupling):** By wrapping the Data Lake URLs in relational Views (`dimCustomer`, `dimProduct`, `dimDate`, `factSales`), the visual report is isolated from the physical folder structure. Any future path or folder change only requires adjusting the View, protecting the Power BI reports from breaking.
- **Operational Efficiency without Dedicated Servers:** Synapse Serverless processes on demand (pay-per-query), eliminating the need to provision and pay for a dedicated relational database running 24/7.

### 🎨 Executive Dashboard

The dashboard was built with a dark corporate layout (*Dark Tech*), organizing visuals into blocks for quick reading of business indicators:

[![Dashboard Preview](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/raw/main/dashboard/dashboard_preview.png)](/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/dashboard/dashboard_preview.png)

### 🎥 Interactive Demo

Check out the real-time navigation and dynamic filter interaction in the report:

**demo_adventureworks_dashboard.mp4** (see `dashboard/` folder)

### 🎯 Dashboard Highlights & KPIs

- **High-Level Metrics (KPIs):** Total Revenue ($708.7K), Average Order Value ($22.1K), Total Orders (32), and Unit Volume (2.1K).
- **Portfolio Analysis:** Sales ranking by product category (highlighting *Touring Bikes* and *Road Bikes*).
- **Time Trend:** Historical month-over-month revenue trend.
- **Customer Performance:** Analytical table grouping sales and order count by partner company.
- **Dynamic Segmentation:** Interactive slicers by year, month, date, customer, product, and category.

---

## 🎯 Business Insights & Analytical Answers

From processing the data in the Lakehouse architecture and consolidating it in Power BI, the following strategic answers were extracted for commercial management:

### 📈 1. Commercial Performance & Profitability

- **Global Metrics:**
  * **Total Revenue:** **$708.7K** ($708,690.20)
  * **Average Order Value:** **$22.1K** ($22,146.57 per order)
  * **Volume and Orders:** **2,087 units sold** across **32 orders**.
- **Top Revenue Categories:**
  1. 🥇 **Touring Bikes:** $220.7K
  2. 🥈 **Road Bikes:** $183.1K
  3. 🥉 **Mountain Bikes:** $170.8K
- **Product Mix:** The complete bicycles category brings the highest gross revenue (high aggregate value), while components (*Mountain/Road Frames*) and apparel (*Jerseys*) make up the secondary turnover line.

### 👥 2. Customer Intelligence (B2B Profile)

- **Purchase Behavior:** Revenue is heavily concentrated among B2B distributors and resellers, showing a lower transaction volume (32 orders) but high value per batch.
- **Top Revenue Customers (High-Value Customers):**
  1. **Action Bicycle Specialists:** $89,869.30 (267 units in 1 order)
  2. **Bulk Discount Store:** $74,160.20 (167 units in 1 order)
  3. **Closest Bicycle Store:** $28,950.70 (76 units in 1 order)

### 📅 3. Time & Seasonality Analysis

- **Historical Trend:** Continuous analysis between **November 2023 and October 2024**, showing strong sales recovery throughout 2024 after a first-quarter dip (Feb–Apr).
- **Seasonal Demand Peaks:**
  * **October 2024 ($169.1K):** Absolute sales record in the project's history.
  * **May 2024 ($131.1K):** Second-highest sales volume.
- **Strategic Insight:** The peaks concentrated in **May** and **October** indicate key restocking moments for the retail market ahead of high-demand seasons.

---

⬅️ Back to the [main README](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/README.md) · See also the [Code Walkthrough (PySpark)](https://github.com/DataDaniels/azure-synapse-medallion-adventureworks/blob/main/docs/CODE_WALKTHROUGH.md)
