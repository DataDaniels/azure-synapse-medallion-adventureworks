# 💻 Code Walkthrough — Technical Decisions in PySpark

🌐 **Language:** [🇺🇸 English](CODE_WALKTHROUGH.md) | [🇧🇷 Português](CODE_WALKTHROUGH.pt-br.md)

> ⬅️ Back to the [main README](../README.md) · See also the [full Architecture](ARCHITECTURE.md)

This document walks through the most strategic snippets from the project's two PySpark notebooks, with comments explaining **what** the code does and, more importantly, **why** each technical decision was made. The blocks can be copied directly for reuse.

---

## 🥈 `notebooks/01_raw_to_enriched.ipynb` — Bronze → Silver

### 1. Dynamic table ingestion (schema-agnostic loading)

```python
%%pyspark

# Lists all files and directories inside the 'raw' container
file_list = mssparkutils.fs.ls("abfss://raw@storage_account.dfs.core.windows.net/")

# Iterates over each file to automatically create Temp Views,
# eliminating the need to hardcode table names in the notebook.
for file_path in file_list:
    print(f"Reading: {file_path.path}")

    # 1. Loads the CSV with automatic schema inference
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(file_path.path)

    # 2. Strips the 'SalesLT.' prefix and '.csv' suffix to name the View
    view_name = file_path.name.replace('SalesLT.', '').removesuffix('.csv')

    # 3. Creates the Temp View, used in the next cells via Spark SQL
    df.createOrReplaceTempView(view_name)
    print(f"View created successfully: {view_name}")
```
> 💡 **Why it's strategic:** the loop decouples the notebook from a fixed list of the 10 `SalesLT` tables. If a new table is added to the Raw layer, it gets read and exposed as a View automatically, with no code changes needed — important for pipeline maintainability.

### 2. Historical series simulation (data augmentation)

```python
from pyspark.sql.functions import rand, col, expr

df_salesorderheader = spark.sql("""
    SELECT SalesOrderID, RevisionNumber, OrderDate, DueDate, ShipDate, Status,
           OnlineOrderFlag, SalesOrderNumber, PurchaseOrderNumber, AccountNumber,
           CustomerID, ShipToAddressID, BillToAddressID, ShipMethod,
           SubTotal, TaxAmt, Freight, TotalDue
    FROM salesorderheader
""")

# The AdventureWorks LT test dataset has a single order date (OrderDate).
# To simulate ~1 year of historical data (needed for seasonality analysis),
# the original column is replaced with a random date within the range
# [today - 1000 days, today - 1000 days + 365 days].
df_salesorderheader = df_salesorderheader.drop("OrderDate").withColumn(
    "OrderDate",
    expr("date_add(current_date() - 1000, CAST(rand() * 365 AS INT))")
)
```
> 💡 **Why it's strategic:** without this step, none of the dashboard's time-based analyses (monthly trend, May/October seasonal peaks) would make sense, since the source database only has one fixed date. This technique is what makes `dimDate` and the seasonality insights described in the [Architecture doc](ARCHITECTURE.md#-business-insights--analytical-answers) possible.

### 3. Persistence to Delta Lake (idempotent pattern)

```python
path = "abfss://enriched@storage_account.dfs.core.windows.net/"
tableName = "salesOrderHeader"

# overwrite + overwriteSchema ensures idempotency: every pipeline run
# reprocesses the data from scratch (no duplicates) and allows schema
# evolution without breaking the job in future runs.
df_salesorderheader.write.mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .save(path + "/" + tableName)
```
> 💡 **Why it's strategic:** this same pattern repeats for all 6 Silver tables (`salesCustomer`, `salesCustomerAddress`, `salesOrderHeader`, `salesOrderDetail`, `salesProduct`, `salesProductCategory`), ensuring the pipeline can be safely re-run — complementing the `Delete Old Data` activity in the Synapse pipeline.

---

## 🥇 `notebooks/02_enriched_to_curated.ipynb` — Silver → Gold

### 4. Surrogate Key generation (the core of the Star Schema)

```python
from pyspark.sql.functions import monotonically_increasing_id

# Joins Customer with CustomerAddress to consolidate the dimension into a single table
df_dimCustomer = spark.sql("""
    SELECT sc.*, sca.AddressID, sca.AddressType
    FROM salesCustomer sc
    JOIN salescustomeraddress sca ON sc.customerid = sca.customerid
""")

# Adds the Surrogate Key as the dimension's first column.
# monotonically_increasing_id() generates a unique, sequential ID per partition,
# decoupling the analytical key (used in the Star Schema) from the natural key
# (CustomerID) coming from the source transactional system.
df_dimCustomer_with_surrogate_key = df_dimCustomer.withColumn(
    "CustomerIDKey", monotonically_increasing_id()
).select(
    "CustomerIDKey",
    *[c for c in df_dimCustomer.columns if c != "CustomerIDKey"]
)

df_dimCustomer_with_surrogate_key.createOrReplaceTempView('dimCustomer')
```
> 💡 **Why it's strategic:** this is the central decision of the dimensional model. Surrogate Keys (`CustomerIDKey`, `ProductIDKey`) decouple the analytical model from the source's business keys, allowing, for example, future tracking of historical attribute changes (SCD Type 2) without breaking relationships already published in Power BI.

### 5. Dynamic generation of the Calendar Dimension (`dimDate`)

```python
from pyspark.sql.functions import expr

start_date = "2000-01-01"
end_date = "2024-12-31"

# Calculates the number of days between the dates and uses spark.range() to
# generate one row per day in the interval — without depending on any source table.
df_dimDate = spark.range(
    0,
    spark.sql(f"SELECT datediff('{end_date}', '{start_date}')").collect()[0][0] + 1
).selectExpr("CAST(id AS INT) AS id") \
 .selectExpr(f"date_add('{start_date}', id) AS Date")

# Derives the calendar attributes used in seasonality analysis
df_dimDate = df_dimDate \
    .withColumn("Year", expr("year(Date)")) \
    .withColumn("Month", expr("month(Date)")) \
    .withColumn("DayOfMonth", expr("dayofmonth(Date)")) \
    .withColumn("DayOfYear", expr("dayofyear(Date)")) \
    .withColumn("WeekOfYear", expr("weekofyear(Date)")) \
    .withColumn("DayOfWeek", expr("dayofweek(Date)")) \
    .withColumn("Quarter", expr("quarter(Date)"))
```
> 💡 **Why it's strategic:** `dimDate` is built independently of `SalesLT` (a dimensional modeling best practice — every Star Schema should have its own calendar dimension). It's what underpins the "Time & Seasonality Analysis" referenced in the business insights.

### 6. Building the Sales Fact table (`factSales`)

```python
# Joins the order header (Order-level granularity) with the order line items
# (OrderDetail-level granularity), resolving the Product and Customer Surrogate
# Keys via LEFT JOIN against the dimensions already processed in this same run.
df_factSales = spark.sql("""
    SELECT dp.ProductIDKey, ds.CustomerIDKey, soh.*,
           sod.OrderQty, sod.ProductID, sod.UnitPrice, sod.UnitPriceDiscount, sod.LineTotal
    FROM salesorderheader soh
    JOIN salesorderdetail sod ON soh.SalesOrderID = sod.SalesOrderID
    LEFT JOIN dimProduct dp ON sod.ProductID = dp.ProductID
    LEFT JOIN dimCustomer ds ON soh.CustomerID = ds.CustomerID
""")
```
> 💡 **Why it's strategic:** this is the central table of the Star Schema, at order-line-item granularity. Using `LEFT JOIN` (instead of `INNER JOIN`) against the dimensions avoids accidentally dropping sales rows if a product or customer FK fails to resolve — prioritizing the integrity of the revenue metric over strict dimensional relationship enforcement.

---

⬅️ Back to the [main README](../README.md) · See also the [full Architecture](ARCHITECTURE.md)
