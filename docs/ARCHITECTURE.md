# 🏗️ Arquitetura, BI & Insights de Negócio

> ⬅️ Voltar ao [README principal](../README.md) · Veja também o [Code Walkthrough (PySpark)](CODE_WALKTHROUGH.md)

Este documento detalha o contexto de negócio, a arquitetura completa da solução, a validação do pipeline no Azure Synapse, a camada de apresentação em Power BI e os insights analíticos extraídos do projeto **AdventureWorks Medallion Pipeline**.

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

> 🔍 Quer ver o código PySpark comentado por trás de cada uma dessas etapas? Veja o [Code Walkthrough](CODE_WALKTHROUGH.md).

---

## 📊 Validação e Histórico de Execução

O pipeline foi homologado com 100% de taxa de sucesso (`Succeeded`) em todas as 14 atividades no Azure Synapse Analytics:

### Fluxo e Execução das Atividades
![Pipeline Flow](../architecture/pipeline_canvas.png)

### Monitoramento e Performance Detalhada
![Pipeline Execution](../architecture/pipeline_execution.png)

- **Limpeza (`Delete Old Data`):** Executada com sucesso para garantir a idempotência do pipeline.
- **Cópia Concorrente (`Copy_tqx` no `ForEach`):** Média de ~20s por tabela via Integration Runtime.
- **Job PySpark (Raw -> Enriched):** Concluído em 4m 07s.
- **Job PySpark (Enriched -> Curated):** Concluído em 3m 37s.

---

## 📊 Camada de Apresentação & Business Intelligence (Power BI)

A camada Gold (`curated`) do Data Lake foi disponibilizada via **Synapse Serverless SQL Pool** e conectada ao **Power BI Desktop** para a construção de um painel executivo e interativo de vendas.

### 📐 Modelagem Dimensional (Star Schema)

No Power BI Desktop, as views do Synapse foram conectadas e organizadas em uma estrutura **Star Schema (1 para Muitos)**, garantindo integridade relacional, alta performance analítica e cálculo eficiente de medidas DAX:

![Data Modeling](../dashboard/data_modeling.png)

- **Tabela Fato (`factSales`):** Centralizada com métricas financeiras e chaves substitutas (*Surrogate Keys*).
- **Tabelas de Dimensão (`dimCustomer`, `dimProduct`, `dimDate`):** Entidades contextuais ligadas à fato pelas chaves relacionais (`customerKey`, `productKey`, `dateKey`).

### 🛠️ Camada de Abstração SQL (Synapse Serverless SQL Pool)

Para conectar o **Power BI Desktop** aos arquivos no formato **Delta Lake** persistidos na camada Gold do Data Lake (ADLS Gen2), disponibilizamos o script `sql/01_create_gold_views.sql`.

Este script é fundamental na arquitetura pelos seguintes motivos técnicos:

- **Ponte entre Arquivos e T-SQL:** O Power BI consome dados de forma otimizada via consultas SQL relacionais. O uso do comando `OPENROWSET` com `FORMAT = 'DELTA'` permite que o motor Serverless leia os arquivos parquet/delta diretamente no repositório de arquivos e os exponha como tabelas nativas de banco de dados.
- **Camada de Abstração (Decoupling):** Ao encapsular as URLs do Data Lake em *Views* relacionais (`dimCustomer`, `dimProduct`, `dimDate`, `factSales`), isolamos o relatório visual da estrutura física de diretórios. Qualquer mudança futura de caminhos ou pastas exige ajuste apenas na View, protegendo os relatórios do Power BI de quebras.
- **Eficiência Operacional sem Servidores Dedicados:** O Synapse Serverless realiza o processamento sob demanda (*pay-per-query*), eliminando a necessidade de provisionar e pagar por um banco de dados relacional dedicado ligado 24/7.

### 🎨 Dashboard Executivo

O painel foi desenvolvido com um layout corporativo escuro (*Dark Tech*), organizando os visuais em blocos para rápida leitura dos indicadores do negócio:

![Dashboard Preview](../dashboard/dashboard_preview.png)

### 🎥 Demonstração Interativa

Confira a navegação em tempo real e a aplicação dos filtros dinâmicos no relatório:

https://github.com/user-attachments/assets/d2af6cb1-4e54-4c98-a634-15281c31b1e8

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

### 👥 2. Inteligência de Clientes (Perfil B2B)
- **Comportamento de Compra:** O faturamento é altamente focado em distribuidores e revendedores B2B, apresentando um volume menor de transações (32 pedidos), porém com alto valor por lote.
- **Top Clientes em Faturamento (*High-Value Customers*):**
  1. **Action Bicycle Specialists:** $89.869,30 (267 unidades em 1 pedido)
  2. **Bulk Discount Store:** $74.160,20 (167 unidades em 1 pedido)
  3. **Closest Bicycle Store:** $28.950,70 (76 unidades em 1 pedido)

### 📅 3. Análise Temporal & Sazonalidade
- **Evolução Histórica:** Análise contínua entre **Novembro de 2023 e Outubro de 2024**, mostrando forte recuperação das vendas ao longo de 2024 após retração no primeiro trimestre (Fev–Abr).
- **Picos Sazonais de Demanda:**
  - **Outubro de 2024 ($169,1K):** Recorde absoluto de vendas no histórico do projeto.
  - **Maio de 2024 ($131,1K):** Segundo maior volume de vendas.
- **Insight Estratégico:** Os picos concentrados nos meses de **Maio** e **Outubro** indicam momentos chave de reposição de estoque do mercado varejista antes de temporadas de alta procura.

---

⬅️ Voltar ao [README principal](../README.md) · Veja também o [Code Walkthrough (PySpark)](CODE_WALKTHROUGH.md)
