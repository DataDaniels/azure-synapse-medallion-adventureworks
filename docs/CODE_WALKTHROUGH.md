# 💻 Code Walkthrough — Decisões Técnicas em PySpark

> ⬅️ Voltar ao [README principal](../README.md) · Veja também a [Arquitetura completa](ARCHITECTURE.md)

Este documento traz os trechos mais estratégicos dos dois notebooks PySpark do projeto, com comentários explicando **o quê** o código faz e, principalmente, **por quê** essa decisão técnica foi tomada. Os blocos podem ser copiados diretamente para reuso.

---

## 🥈 `notebooks/01_raw_to_enriched.ipynb` — Bronze → Silver

### 1. Ingestão dinâmica das tabelas (schema-agnostic loading)

```python
%%pyspark

# Lista todos os arquivos e diretórios dentro do container 'raw'
file_list = mssparkutils.fs.ls("abfss://raw@storage_account.dfs.core.windows.net/")

# Itera sobre cada arquivo para criar as Temp Views automaticamente,
# eliminando a necessidade de hardcode de nomes de tabelas no notebook.
for file_path in file_list:
    print(f"Lendo: {file_path.path}")

    # 1. Carrega o CSV com inferência automática de schema
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(file_path.path)

    # 2. Remove o prefixo 'SalesLT.' e o sufixo '.csv' para nomear a View
    view_name = file_path.name.replace('SalesLT.', '').removesuffix('.csv')

    # 3. Cria a View Temporária, usada nas próximas células via Spark SQL
    df.createOrReplaceTempView(view_name)
    print(f"View criada com sucesso: {view_name}")
```
> 💡 **Por que é estratégico:** o loop desacopla o notebook da lista fixa das 10 tabelas do `SalesLT`. Se uma nova tabela for adicionada na camada Raw, ela é lida e disponibilizada como View automaticamente, sem precisar tocar no código — importante para manutenibilidade do pipeline.

### 2. Simulação de série histórica (data augmentation)

```python
from pyspark.sql.functions import rand, col, expr

df_salesorderheader = spark.sql("""
    SELECT SalesOrderID, RevisionNumber, OrderDate, DueDate, ShipDate, Status,
           OnlineOrderFlag, SalesOrderNumber, PurchaseOrderNumber, AccountNumber,
           CustomerID, ShipToAddressID, BillToAddressID, ShipMethod,
           SubTotal, TaxAmt, Freight, TotalDue
    FROM salesorderheader
""")

# O dataset de testes AdventureWorks LT possui uma única data de pedido (OrderDate).
# Para simular uma massa histórica de ~1 ano (necessária para análises de sazonalidade),
# a coluna original é substituída por uma data aleatória no intervalo
# [hoje - 1000 dias, hoje - 1000 dias + 365 dias].
df_salesorderheader = df_salesorderheader.drop("OrderDate").withColumn(
    "OrderDate",
    expr("date_add(current_date() - 1000, CAST(rand() * 365 AS INT))")
)
```
> 💡 **Por que é estratégico:** sem esse tratamento, todas as análises temporais do dashboard (evolução mensal, picos sazonais em Maio/Outubro) não fariam sentido, pois o banco de origem tem apenas uma data fixa. É essa técnica que viabiliza a `dimDate` e os insights de sazonalidade descritos na [Arquitetura](ARCHITECTURE.md#-insights-de-negócio--respostas-analíticas).

### 3. Persistência em Delta Lake (padrão idempotente)

```python
path = "abfss://enriched@storage_account.dfs.core.windows.net/"
tableName = "salesOrderHeader"

# overwrite + overwriteSchema garante idempotência: cada execução do pipeline
# reprocessa o dado do zero (sem duplicações) e permite evolução de schema
# sem quebrar o job em execuções futuras.
df_salesorderheader.write.mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .save(path + "/" + tableName)
```
> 💡 **Por que é estratégico:** esse mesmo padrão se repete para as 6 tabelas Silver (`salesCustomer`, `salesCustomerAddress`, `salesOrderHeader`, `salesOrderDetail`, `salesProduct`, `salesProductCategory`), garantindo que o pipeline seja re-executável com segurança — complementando a atividade `Delete Old Data` do pipeline no Synapse.

---

## 🥇 `notebooks/02_enriched_to_curated.ipynb` — Silver → Gold

### 4. Geração de Surrogate Keys (núcleo do Star Schema)

```python
from pyspark.sql.functions import monotonically_increasing_id

# Junta Customer com CustomerAddress para consolidar a dimensão em uma única tabela
df_dimCustomer = spark.sql("""
    SELECT sc.*, sca.AddressID, sca.AddressType
    FROM salesCustomer sc
    JOIN salescustomeraddress sca ON sc.customerid = sca.customerid
""")

# Adiciona a Surrogate Key como primeira coluna da dimensão.
# monotonically_increasing_id() gera um ID único e sequencial por partição,
# desacoplando a chave analítica (usada no Star Schema) da chave natural
# (CustomerID) vinda do sistema transacional de origem.
df_dimCustomer_with_surrogate_key = df_dimCustomer.withColumn(
    "CustomerIDKey", monotonically_increasing_id()
).select(
    "CustomerIDKey",
    *[c for c in df_dimCustomer.columns if c != "CustomerIDKey"]
)

df_dimCustomer_with_surrogate_key.createOrReplaceTempView('dimCustomer')
```
> 💡 **Por que é estratégico:** essa é a decisão central da modelagem dimensional. As Surrogate Keys (`CustomerIDKey`, `ProductIDKey`) desacoplam o modelo analítico das chaves de negócio da origem, permitindo, por exemplo, futuramente rastrear mudanças históricas de atributos (SCD Tipo 2) sem quebrar os relacionamentos já publicados no Power BI.

### 5. Geração dinâmica da Dimensão Calendário (`dimDate`)

```python
from pyspark.sql.functions import expr

start_date = "2000-01-01"
end_date = "2024-12-31"

# Calcula a quantidade de dias entre as datas e usa spark.range() para
# gerar uma linha por dia no intervalo — sem depender de nenhuma tabela de origem.
df_dimDate = spark.range(
    0,
    spark.sql(f"SELECT datediff('{end_date}', '{start_date}')").collect()[0][0] + 1
).selectExpr("CAST(id AS INT) AS id") \
 .selectExpr(f"date_add('{start_date}', id) AS Date")

# Deriva os atributos de calendário usados nas análises de sazonalidade
df_dimDate = df_dimDate \
    .withColumn("Year", expr("year(Date)")) \
    .withColumn("Month", expr("month(Date)")) \
    .withColumn("DayOfMonth", expr("dayofmonth(Date)")) \
    .withColumn("DayOfYear", expr("dayofyear(Date)")) \
    .withColumn("WeekOfYear", expr("weekofyear(Date)")) \
    .withColumn("DayOfWeek", expr("dayofweek(Date)")) \
    .withColumn("Quarter", expr("quarter(Date)"))
```
> 💡 **Por que é estratégico:** a `dimDate` é construída de forma independente do `SalesLT` (boa prática de modelagem dimensional — toda Star Schema deve ter uma dimensão calendário própria). É ela que sustenta a "Análise Temporal & Sazonalidade" citada nos insights de negócio.

### 6. Construção da tabela Fato de Vendas (`factSales`)

```python
# Junta o cabeçalho do pedido (granularidade "Order") com os itens do pedido
# (granularidade "OrderDetail"), resolvendo as Surrogate Keys de Produto e
# Cliente via LEFT JOIN contra as dimensões já processadas nesta mesma execução.
df_factSales = spark.sql("""
    SELECT dp.ProductIDKey, ds.CustomerIDKey, soh.*,
           sod.OrderQty, sod.ProductID, sod.UnitPrice, sod.UnitPriceDiscount, sod.LineTotal
    FROM salesorderheader soh
    JOIN salesorderdetail sod ON soh.SalesOrderID = sod.SalesOrderID
    LEFT JOIN dimProduct dp ON sod.ProductID = dp.ProductID
    LEFT JOIN dimCustomer ds ON soh.CustomerID = ds.CustomerID
""")
```
> 💡 **Por que é estratégico:** é a tabela central do Star Schema, na granularidade de item de pedido. O uso de `LEFT JOIN` (em vez de `INNER JOIN`) nas dimensões evita a perda acidental de linhas de vendas caso alguma FK de produto ou cliente não seja resolvida — priorizando a integridade da métrica de faturamento sobre a obrigatoriedade do relacionamento dimensional.

---

⬅️ Voltar ao [README principal](../README.md) · Veja também a [Arquitetura completa](ARCHITECTURE.md)
