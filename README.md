# 🚀 Azure Synapse Medallion Pipeline (AdventureWorks)

Este repositório contém a implementação de uma pipeline end-to-end de Engenharia de Dados desenvolvida no **Azure Synapse Analytics**, aplicando a **Medallion Architecture (Bronze, Silver e Gold)** com **Delta Lake** no **Azure Data Lake Storage Gen2 (ADLS Gen2)**.

![Pipeline Canvas](architecture/pipeline_flow.png)

O projeto realiza a ingestão paralela de dados a partir de um banco relacional **Azure SQL Database**, trata e padroniza as informações na camada Silver e constrói um modelo dimensional **Star Schema** na camada Gold via **PySpark**, deixando a estrutura pronta para análises e relatórios no **Power BI**.

---

## 🎯 Contexto de Negócio & Objetivos Analíticos

Mais do que uma solução técnica robusta em nuvem, este projeto foi desenhado para resolver um desafio clássico de negócio: **transformar dados operacionais (OLTP) de vendas e clientes em inteligência estratégica pronta para tomada de decisão.**

A base processada pertence ao ecossistema **AdventureWorks** (fabricante e distribuidora global de bicicletas, peças e acessórios). A estrutura dimensional criada na camada Gold (`curated`) permite responder diretamente às seguintes perguntas estratégicas:

- **Desempenho Comercial & Lucratividade:**
  - Qual é a Receita Total ($), Ticket Médio e Volume de Vendas por período?
  - Quais são os produtos e categorias mais rentáveis e de maior giro?
- **Inteligência de Clientes:**
  - Quem são os clientes de alto valor (*High-Value Customers*)?
- **Análise Temporal & Sazonalidade:**
  - Qual é a evolução histórica das vendas ao longo dos meses e anos (suportada pela `dimDate`)?
  - Existem picos sazonais de demanda para categorias específicas de produtos?

---

## 🏗️ Arquitetura da Solução

O fluxo de dados foi construído seguindo o padrão de pipeline end-to-end na nuvem:

| Etapa | Componente Principal | Descrição da Operação | Formato / Saída |
| :--- | :--- | :--- | :--- |
| **🛢️ Origem** | **Azure SQL Database** | Fonte de dados relacional (Schema `SalesLT`). | Tabelas OLTP |
| **🔄 Ingestão** | **Synapse Pipeline** | Execução de atividade `Delete` para idempotência e `ForEach` para cópia paralela. | Arquivos CSV (`raw/`) |
| **🥈 Silver** | **PySpark** *(Spark Pool)* | Limpeza de schemas, tratamento de colunas e simulação de datas históricas. | Tabelas Delta (`enriched/`) |
| **🥇 Gold** | **PySpark / Spark SQL** | Modelagem Star Schema com *Surrogate Keys* e Dimensão Calendário. | Tabelas Delta (`curated/`) |
| **📊 Consumo** | **Power BI / Analytics** | Camada final pronta para modelagem analítica e dashboards. | Dashboards & Reports |

---

## ⚙️ Tecnologias & Serviços Azure Utilizados

- **Azure Synapse Analytics:** Ambiente integrado para orquestração de pipelines e processamento Big Data.
- **Apache Spark Pool:** Cluster gerenciado para execução dos jobs de transformação em PySpark.
- **Azure Data Lake Storage Gen2 (ADLS Gen2):** Armazenamento em nuvem otimizado para analytics com namespace hierárquico.
- **Delta Lake:** Formato de armazenamento open-source que traz transações ACID e alto desempenho para o Data Lake.
- **Azure SQL Database:** Banco de dados relacional de origem (Base de testes AdventureWorks LT).
- **Azure RBAC & Managed Identity:** Controle de acesso unificado e seguro entre serviços sem exposição de chaves.

---

## 🗄️ Estrutura do Data Lake & Arquitetura Medalhão

### 1. Camada Raw / Bronze (`raw/`)
- **Formato:** CSV
- Contém a ingestão bruta das 10 tabelas do schema `SalesLT` (`SalesLT.Customer.csv`, `SalesLT.Product.csv`, `SalesLT.SalesOrderHeader.csv`, etc.).

### 2. Camada Enriched / Silver (`enriched/`)
- **Formato:** Delta Lake
- Processamento via notebook PySpark `01_raw_to_enriched`:
  - Limpeza de colunas desnecessárias e renomeação de atributos.
  - Randomização de datas na coluna `OrderDate` para simular massa histórica real.
  - Salvamento como tabelas **Delta Lake**: `salesCustomer`, `salesCustomerAddress`, `salesOrderHeader`, `salesOrderDetail`, `salesProduct`, `salesProductCategory`.

### 3. Camada Curated / Gold (`curated/`)
- **Formato:** Delta Lake (Modelo Dimensional Star Schema)
- Processamento via notebook PySpark `02_enriched_to_curated`:
  - Criação de *Surrogate Keys* (`monotonically_increasing_id()`).
  - Geração dinâmica da Dimensão Tempo (`dimDate`).
  - Criação das tabelas no formato **Delta Lake**:
    - **`dimCustomer`**: Dimensão Cliente tratada com endereço e chave substituta.
    - **`dimProduct`**: Dimensão Produto consolidada com categoria e subcategoria.
    - **`dimDate`**: Dimensão Calendário cobrindo o período de análises.
    - **`factSales`**: Tabela Fato de Vendas combinando cabeçalho e itens de pedidos com as chaves das dimensões.

---

## 💻 Destaques de Código (PySpark) & Decisões Técnicas

Abaixo estão os trechos mais estratégicos dos dois notebooks do projeto, com comentários explicando **o quê** o código faz e, principalmente, **por quê** essa decisão técnica foi tomada. Os blocos podem ser copiados diretamente para reuso.

### 🥈 `01_raw_to_enriched.ipynb` — Bronze → Silver

**1. Ingestão dinâmica das tabelas (schema-agnostic loading)**

```python
%%pyspark

# Lista todos os arquivos e diretórios dentro do container 'raw'
file_list = mssparkutils.fs.ls("abfss://raw@<storage_account>.dfs.core.windows.net/")

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

**2. Simulação de série histórica (data augmentation)**

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
> 💡 **Por que é estratégico:** sem esse tratamento, todas as análises temporais do dashboard (evolução mensal, picos sazonais em Maio/Outubro) não fariam sentido, pois o banco de origem tem apenas uma data fixa. É essa técnica que viabiliza a `dimDate` e os insights de sazonalidade descritos mais abaixo.

**3. Persistência em Delta Lake (padrão idempotente)**

```python
path = "abfss://enriched@<storage_account>.dfs.core.windows.net/"
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

### 🥇 `02_enriched_to_curated.ipynb` — Silver → Gold

**4. Geração de Surrogate Keys (núcleo do Star Schema)**

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

**5. Geração dinâmica da Dimensão Calendário (`dimDate`)**

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
> 💡 **Por que é estratégico:** a `dimDate` é construída de forma independente do `SalesLT` (boa prática de modelagem dimensional — toda Star Schema deve ter uma dimensão calendário própria). É ela que sustenta a "Análise Temporal & Sazonalidade" citada nos insights de negócio do dashboard.

**6. Construção da tabela Fato de Vendas (`factSales`)**

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

## 📊 Validação e Histórico de Execução

O pipeline foi homologado com 100% de taxa de sucesso (`Succeeded`) em todas as 14 atividades no Azure Synapse Analytics:

### Fluxo e Execução das Atividades
![Pipeline Flow](architecture/pipeline_canvas.png)

### Monitoramento e Performance Detalhada
![Pipeline Execution](architecture/pipeline_execution.png)

- **Limpeza (`Delete Old Data`):** Executada com sucesso para garantir a idempotência do pipeline.
- **Cópia Concorrente (`Copy_tqx` no `ForEach`):** Média de ~20s por tabela via Integration Runtime.
- **Job PySpark (Raw -> Enriched):** Concluído em 4m 07s.
- **Job PySpark (Enriched -> Curated):** Concluído em 3m 37s.

---

## 📊 Camada de Apresentação & Business Intelligence (Power BI)

A camada Gold (`curated`) do Data Lake foi disponibilizada via **Synapse Serverless SQL Pool** e conectada ao **Power BI Desktop** para a construção de um painel executivo e interativo de vendas.

---

### 📐 Modelagem Dimensional (Star Schema)

No Power BI Desktop, as views do Synapse foram conectadas e organizadas em uma estrutura **Star Schema (1 para Muitos)**, garantindo integridade relacional, alta performance analítica e cálculo eficiente de medidas DAX:

![Data Modeling](dashboard/data_modeling.png)

- **Tabela Fato (`factSales`):** Centralizada com métricas financeiras e chaves substitutas (*Surrogate Keys*).
- **Tabelas de Dimensão (`dimCustomer`, `dimProduct`, `dimDate`):** Entidades contextuais ligadas à fato pelas chaves relacionais (`customerKey`, `productKey`, `dateKey`).

---

### 🛠️ Camada de Abstração SQL (Synapse Serverless SQL Pool)

Para conectar o **Power BI Desktop** aos arquivos no formato **Delta Lake** persistidos na camada Gold do Data Lake (ADLS Gen2), disponibilizamos o script `sql/01_create_gold_views.sql`[cite: 1]. 

Este script é fundamental na arquitetura pelos seguintes motivos técnicos:

- **Ponte entre Arquivos e T-SQL:** O Power BI consome dados de forma otimizada via consultas SQL relacionais. O uso do comando `OPENROWSET` com `FORMAT = 'DELTA'` permite que o motor Serverless leia os arquivos parquet/delta diretamente no repositório de arquivos[cite: 1] e os exponha como tabelas nativas de banco de dados.
- **Camada de Abstração (Decoupling):** Ao encapsular as URLs do Data Lake em *Views* relacionais (`dimCustomer`, `dimProduct`, `dimDate`, `factSales`)[cite: 1], isolamos o relatório visual da estrutura física de diretórios. Qualquer mudança futura de caminhos ou pastas exige ajuste apenas na View, protegendo os relatórios do Power BI de quebras.
- **Eficiência Operacional sem Servidores Dedicados:** O Synapse Serverless realiza o processamento sob demanda (*pay-per-query*), eliminando a necessidade de provisionar e pagar por um banco de dados relacional dedicado ligado 24/7.

---

### 🎨 Dashboard Executivo

O painel foi desenvolvido com um layout corporativo escuro (*Dark Tech*), organizando os visuais em blocos para rápida leitura dos indicadores do negócio:

![Dashboard Preview](dashboard/dashboard_preview.png)

---

### 🎥 Demonstração Interativa

Confira a navegação em tempo real e a aplicação dos filtros dinâmicos no relatório:

https://github.com/user-attachments/assets/d2af6cb1-4e54-4c98-a634-15281c31b1e8


---

### 🎯 Destaques e KPIs do Painel

- **Métricas de Alto Nível (KPIs):** Acompanhamento do Faturamento Total ($708,7K), Ticket Médio ($22,1K), Total de Pedidos (32) e Volume de Unidades (2,1K).
- **Análise de Portfólio:** Ranking de vendas por categoria de produto (com destaque para *Touring Bikes* e *Road Bikes*).
- **Evolução Temporal:** Análise histórica da curva de faturamento mês a mês.
- **Performance por Cliente:** Tabela analítica agrupando vendas e quantidade de pedidos por empresa parceira.
- **Segmentação Dinâmica:** Slicers interativos por ano, mês, data, cliente, produto e categoria.


---

## 🎯 Insights de Negócio & Respostas Analíticas

A partir do processamento dos dados na arquitetura Lakehouse e da consolidação no Power BI, foram extraídas as seguintes respostas estratégicas para a gestão comercial:

### 📈 1. Desempenho Comercial & Lucratividade
- **Métricas Globais:**
  - **Faturamento Total:** **$708,7K** ($708.690,20)
  - **Ticket Médio:** **$22,1K** ($22.146,57 por pedido)
  - **Volume e Pedidos:** **2.087 unidades vendidas** distribuídas em **32 pedidos**.
- **Categorias de Maior Receita:**
  1. 🥇 **Touring Bikes:** $220,7K
  2. 🥈 **Road Bikes:** $183,1K
  3. 🥉 **Mountain Bikes:** $170,8K
- **Mix de Produtos:** A categoria de bicicletas completas traz o maior faturamento bruto (alto valor agregado), enquanto componentes (*Mountain/Road Frames*) e vestuário (*Jerseys*) compõem a linha de giro secundário.

---

### 👥 2. Inteligência de Clientes (Perfil B2B)
- **Comportamento de Compra:** O faturamento é altamente focado em distribuidores e revendedores B2B, apresentando um volume menor de transações (32 pedidos), porém com alto valor por lote.
- **Top Clientes em Faturamento (*High-Value Customers*):**
  1. **Action Bicycle Specialists:** $89.869,30 (267 unidades em 1 pedido)
  2. **Bulk Discount Store:** $74.160,20 (167 unidades em 1 pedido)
  3. **Closest Bicycle Store:** $28.950,70 (76 unidades em 1 pedido)

---

### 📅 3. Análise Temporal & Sazonalidade
- **Evolução Histórica:** Análise contínua entre **Novembro de 2023 e Outubro de 2024**, mostrando forte recuperação das vendas ao longo de 2024 após retração no primeiro trimestre (Fev–Abr).
- **Picos Sazonais de Demanda:**
  - **Outubro de 2024 ($169,1K):** Recorde absoluto de vendas no histórico do projeto.
  - **Maio de 2024 ($131,1K):** Segundo maior volume de vendas.
- **Insight Estratégico:** Os picos concentrados nos meses de **Maio** e **Outubro** indicam momentos chave de reposição de estoque do mercado varejista antes de temporadas de alta procura.


---


### 📁 Estrutura do Repositório

```text
azure-synapse-medallion-adventureworks/
│
├── README.md                           # Documentação principal do projeto
│
├── architecture/
│   ├── pipeline_canvas.png             # Interface do Synapse com o pipeline em destaque
│   ├── pipeline_execution.png          # Monitoramento detalhado das execuções (Status Succeeded)
│   └── pipeline_flow.png               # Fluxo visual das atividades concluídas
│
├── dashboard/
│   ├── ADVENTUREWORKS_DASHBOARD.pbix   # Arquivo do relatório no Power BI Desktop
│   ├── background.png                  # Plano de fundo personalizado estilo Dark Tech
│   ├── dashboard_preview.png           # Captura de tela do painel executivo final
│   ├── data_modeling.png               # Diagrama do modelo relacional Star Schema
│   └── demo_adventureworks_dashboard.mp4 # Vídeo de demonstração interativa do dashboard
│
├── notebooks/
│   ├── 01_raw_to_enriched.ipynb        # Código PySpark (Bronze -> Silver em formato Delta)
│   └── 02_enriched_to_curated.ipynb    # Código PySpark (Silver -> Gold / Star Schema)
│
├── pipelines/
│   └── End_to_End_Data_Pipeline.json   # Definição e orquestração do pipeline no Synapse
│
└── sql/
    └── 01_create_gold_views.sql        # Script T-SQL para criação das views no Synapse Serverless

## 👨‍💻 Autor
**Daniel Moreira** | Data Engineer
