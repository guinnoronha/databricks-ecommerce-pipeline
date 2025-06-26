# [PT-BR] Pipeline de Engenharia de Dados: Análise de Vendas de E-commerce no Databricks

-----

## 🚀 Visão Geral do Projeto

Este projeto demonstra a construção de um pipeline de engenharia de dados **end-to-end** no **Databricks Community Edition**, focado na ingestão, limpeza, transformação e agregação de dados de vendas de e-commerce. A arquitetura segue o padrão **Data Lakehouse**, com camadas Bronze, Silver e Gold, utilizando **Apache Spark** e **Delta Lake** para garantir escalabilidade, confiabilidade e desempenho.

O objetivo principal é transformar dados brutos de transações de vendas em produtos de dados prontos para análise e consumo por áreas de negócio ou ferramentas de Business Intelligence (BI).

-----

## 💡 Tecnologias Utilizadas

  * **Databricks Community Edition:** Plataforma unificada para engenharia de dados.
  * **Apache Spark:** Motor de processamento de dados distribuído e escalável.
  * **Delta Lake:** Formato de armazenamento de dados open-source que traz capacidades de ACID transactions, schema enforcement e time travel para data lakes.
  * **PySpark / Spark SQL:** APIs do Spark para manipulação e transformação de dados.
  * **Python:** Linguagem de programação principal para os notebooks.
  * **Git / GitHub:** Controle de versão e colaboração.

-----

## 🏗️ Arquitetura do Pipeline (Data Lakehouse)

O pipeline é estruturado em três camadas distintas, cada uma com um propósito específico:

1.  **Bronze Layer (Raw Data):**

      * **Propósito:** Ingestão de dados brutos da fonte (CSV) para o Data Lakehouse.
      * **Características:** Dados quase intocados, mantendo o formato original, porém armazenados como tabelas Delta para durabilidade e rastreabilidade.
      * **Notebook:** `01_bronze_layer_ingestion.py`

2.  **Silver Layer (Cleaned & Conformed Data):**

      * **Propósito:** Limpeza, padronização e enriquecimento dos dados da camada Bronze.
      * **Características:** Tratamento de valores nulos, remoção de duplicatas, correção de tipos de dados, filtragem de registros inválidos e criação de colunas derivadas (`total_price`).
      * **Notebook:** `02_silver_layer_transformation.py`

3.  **Gold Layer (Curated & Aggregated Data):**

      * **Propósito:** Criação de produtos de dados agregados e otimizados para consumo analítico.
      * **Características:** Dados sumarizados para análises de negócio, como vendas por produto, vendas diárias e top clientes. Ideais para dashboards de BI.
      * **Notebook:** `03_gold_layer_product.py`

-----

## 📊 Produtos de Dados (Camada Gold)

Este pipeline gera as seguintes tabelas analíticas na camada Gold:

  * **`sales_by_product`:** Vendas totais (quantidade e receita) por cada item/produto.
  * **`daily_sales`:** Receita diária, contagem de transações e número de clientes únicos por dia.
  * **`top_customers`:** Lista dos clientes que mais gastaram, com o total da receita gerada e o número de faturas.

-----

## ⚙️ Como Executar o Projeto

Para replicar e executar este pipeline no seu ambiente Databricks Community Edition:

### 1\. Configuração do Databricks

  * **Crie uma Conta:** Acesse [Databricks Community Edition](https://community.cloud.databricks.com/) e crie sua conta gratuita.
  * **Crie um Cluster:**
      * No Databricks Workspace, vá em **"Compute"**.
      * Clique em **"+ Create Cluster"**.
      * Nomeie o cluster (ex: `ecommerce-project-cluster`).
      * Escolha a versão mais recente do **Databricks Runtime (LTS)**.
      * O cluster será automaticamente encerrado após um período de inatividade (padrão de 120 minutos) para economizar recursos da versão gratuita.

### 2\. Obtenção e Upload dos Dados

  * **Baixe o Dataset:** Faça o download do dataset "Online Retail" do Kaggle: [Online Retail Dataset](https://www.kaggle.com/datasets/carrie1/ecommerce-data). O arquivo é `Online Retail.xlsx`.
  * **Converta para CSV:** Abra o arquivo `.xlsx` em um editor de planilhas (Excel, Google Sheets) e salve a primeira aba como `online_retail.csv`.
  * **Faça Upload para o DBFS/Volumes:**
      * No Databricks Workspace, vá em **"Catalog"** (ou "Data").
      * Clique em **"Upload to Volume"**.
      * Selecione seu cluster.
      * **Destination path:** Crie um caminho para seus dados brutos. Recomenda-se `/Volumes/main/default/ecommerce_data/raw/` se a funcionalidade `Volumes` estiver disponível. Caso contrário, use `dbfs:/FileStore/datasets/ecommerce_data/raw/`.
      * Arraste e solte o arquivo `online_retail.csv` ou clique para selecioná-lo e finalize o upload.

### 3\. Importação dos Notebooks

  * **Baixe os Notebooks:** Clone este repositório para sua máquina local ou baixe os arquivos `.py` da pasta `notebooks/`.
  * **Importe para o Databricks:**
      * No Databricks Workspace, vá em **"Workspace"**.
      * Clique com o botão direito na sua pasta de usuário (ou em "Shared") e selecione **"Import"**.
      * Escolha **"File"** e faça upload de cada arquivo `.py` (ex: `01_bronze_layer_ingestion.py`, `02_silver_layer_transformation.py`, `03_gold_layer_product.py`). Certifique-se de que o **"Import as"** esteja configurado para **"Notebook"**.

### 4\. Execução do Pipeline

  * **Anexe ao Cluster:** Abra cada notebook e certifique-se de que ele esteja anexado ao cluster que você criou.
  * **Execute em Ordem:** Execute os notebooks na seguinte sequência:
    1.  `01_bronze_layer_ingestion.py`
    2.  `02_silver_layer_transformation.py`
    3.  `03_gold_layer_product.py`

Monitore a execução de cada notebook e verifique as saídas para garantir que o pipeline está funcionando corretamente. As tabelas Delta serão criadas no `dbfs:/user/hive/warehouse/ecommerce_data/` (ou caminho similar) e poderão ser consultadas via SQL ou PySpark.

-----

## 📈 Visualização e Análise

Após a execução do pipeline, as tabelas na camada Gold (`sales_by_product`, `daily_sales`, `top_customers`) estarão prontas para análise. Você pode usar as capacidades de visualização embutidas no Databricks diretamente nas tabelas ou exportar os dados para ferramentas de BI externas (Power BI, Tableau, Looker) para criar dashboards interativos.

Exemplo de consulta no Databricks:

```sql
SELECT description, total_revenue
FROM sales_by_product
ORDER BY total_revenue DESC
LIMIT 20;
```

-----

## 🚀 Próximos Passos e Melhorias

Este projeto pode ser expandido com as seguintes funcionalidades:

  * **Simulação de Streaming com Auto Loader:** Implementar o Auto Loader no notebook da camada Bronze para simular a ingestão contínua de novos arquivos de vendas.
  * **Validação de Qualidade de Dados:** Adicionar mais verificações de qualidade de dados na camada Silver (ex: validações de consistência, detecção de anomalias).
  * **Otimização de Desempenho:** Explorar técnicas como particionamento de tabelas Delta, Z-ordering e compactação para otimizar o desempenho de consulta.
  * **Integração com Ferramentas de BI:** Conectar as tabelas Gold diretamente a ferramentas de BI para criar dashboards de vendas em tempo real.
  * **Orquestração de Jobs:** Utilizar Databricks Workflows (Jobs) para agendar e orquestrar a execução do pipeline automaticamente.

-----

**Autor:** Guilherme Noronha Mello -> github.com/guinnoronha
**Data:** Junho de 2025

-----


# [EN-US] Data Engineering Pipeline: E-commerce Sales Analysis on Databricks

-----

## 🚀 Project Overview

This project demonstrates the construction of an **end-to-end data engineering pipeline** on **Databricks Community Edition**, focusing on the ingestion, cleansing, transformation, and aggregation of e-commerce sales data. The architecture follows the **Data Lakehouse** pattern, with Bronze, Silver, and Gold layers, leveraging **Apache Spark** and **Delta Lake** to ensure scalability, reliability, and performance.

The primary goal is to transform raw sales transaction data into ready-to-use data products for business analysis or Business Intelligence (BI) tools.

-----

## 💡 Technologies Used

  * **Databricks Community Edition:** Unified analytics platform for data engineering.
  * **Apache Spark:** Distributed and scalable data processing engine.
  * **Delta Lake:** Open-source storage format that brings ACID transactions, schema enforcement, and time travel capabilities to data lakes.
  * **PySpark / Spark SQL:** Spark APIs for data manipulation and transformation.
  * **Python:** Primary programming language for the notebooks.
  * **Git / GitHub:** Version control and collaboration.

-----

## 🏗️ Pipeline Architecture (Data Lakehouse)

The pipeline is structured into three distinct layers, each serving a specific purpose:

1.  **Bronze Layer (Raw Data):**

      * **Purpose:** Ingest raw data from the source (CSV) into the Data Lakehouse.
      * **Characteristics:** Data remains largely untouched, maintaining its original format, but stored as Delta tables for durability and traceability.
      * **Notebook:** `01_bronze_layer_ingestion.py`

2.  **Silver Layer (Cleaned & Conformed Data):**

      * **Purpose:** Cleanse, standardize, and enrich the data from the Bronze layer.
      * **Characteristics:** Handles null values, removes duplicates, corrects data types, filters invalid records, and creates derived columns (`total_price`).
      * **Notebook:** `02_silver_layer_transformation.py`

3.  **Gold Layer (Curated & Aggregated Data):**

      * **Purpose:** Create aggregated data products optimized for analytical consumption.
      * **Characteristics:** Summarized data for business analysis, such as sales by product, daily sales, and top customers. Ideal for BI dashboards.
      * **Notebook:** `03_gold_layer_product.py`

-----

## 📊 Data Products (Gold Layer)

This pipeline generates the following analytical tables in the Gold layer:

  * **`sales_by_product`:** Total sales (quantity and revenue) for each item/product.
  * **`daily_sales`:** Daily revenue, transaction count, and unique customer count by day.
  * **`top_customers`:** A list of top-spending customers, including total revenue generated and the number of invoices.

-----

## ⚙️ How to Run the Project

To replicate and run this pipeline in your Databricks Community Edition environment:

### 1\. Databricks Setup

  * **Create an Account:** Go to [Databricks Community Edition](https://community.cloud.databricks.com/) and create your free account.
  * **Create a Cluster:**
      * In your Databricks Workspace, navigate to **"Compute"**.
      * Click on **"+ Create Cluster"**.
      * Name your cluster (e.g., `ecommerce-project-cluster`).
      * Choose the latest **Databricks Runtime (LTS)** version.
      * The cluster will automatically terminate after a period of inactivity (default 120 minutes) to conserve resources in the free version.

### 2\. Data Acquisition and Upload

  * **Download the Dataset:** Download the "Online Retail" dataset from Kaggle: [Online Retail Dataset](https://www.kaggle.com/datasets/carrie1/ecommerce-data). The file is `Online Retail.xlsx`.
  * **Convert to CSV:** Open the `.xlsx` file in a spreadsheet editor (Excel, Google Sheets) and save the first tab as `online_retail.csv`.
  * **Upload to DBFS/Volumes:**
      * In your Databricks Workspace, go to **"Catalog"** (or "Data").
      * Click on **"Upload to Volume"**.
      * Select your cluster.
      * **Destination path:** Create a path for your raw data. `/Volumes/main/default/ecommerce_data/raw/` is recommended if the `Volumes` feature is available. Otherwise, use `dbfs:/FileStore/datasets/ecommerce_data/raw/`.
      * Drag and drop your `online_retail.csv` file or click to select it, then finalize the upload.

### 3\. Notebook Importation

  * **Download the Notebooks:** Clone this repository to your local machine or download the `.py` files from the `notebooks/` folder.
  * **Import into Databricks:**
      * In your Databricks Workspace, go to **"Workspace"**.
      * Right-click on your user folder (or "Shared") and select **"Import"**.
      * Choose **"File"** and upload each `.py` file (e.g., `01_bronze_layer_ingestion.py`, `02_silver_layer_transformation.py`, `03_gold_layer_product.py`). Make sure **"Import as"** is set to **"Notebook"**.

### 4\. Pipeline Execution

  * **Attach to Cluster:** Open each notebook and ensure it's attached to the cluster you created.
  * **Execute in Order:** Run the notebooks in the following sequence:
    1.  `01_bronze_layer_ingestion.py`
    2.  `02_silver_layer_transformation.py`
    3.  `03_gold_layer_product.py`

Monitor each notebook's execution and check the outputs to ensure the pipeline is running correctly. The Delta tables will be created under `dbfs:/user/hive/warehouse/ecommerce_data/` (or a similar path) and can be queried via SQL or PySpark.

-----

## 📈 Visualization and Analysis

After pipeline execution, the tables in the Gold layer (`sales_by_product`, `daily_sales`, `top_customers`) will be ready for analysis. You can use Databricks' built-in visualization capabilities directly on the tables or export the data to external BI tools (Power BI, Tableau, Looker) to create interactive dashboards.

Example query in Databricks:

```sql
SELECT description, total_revenue
FROM sales_by_product
ORDER BY total_revenue DESC
LIMIT 20;
```

-----

## 🚀 Next Steps and Improvements

This project can be expanded with the following functionalities:

  * **Streaming Simulation with Auto Loader:** Implement Auto Loader in the Bronze layer notebook to simulate continuous ingestion of new sales files.
  * **Data Quality Validation:** Add more robust data quality checks in the Silver layer (e.g., consistency validations, anomaly detection).
  * **Performance Optimization:** Explore techniques like Delta table partitioning, Z-ordering, and compaction to optimize query performance.
  * **BI Tool Integration:** Connect Gold layer tables directly to BI tools for real-time sales dashboards.
  * **Job Orchestration:** Utilize Databricks Workflows (Jobs) to schedule and orchestrate the pipeline execution automatically.

-----

**Author:** Guilherme Noronha Mello -> github.com/guinnoronha
**Date:** June 2025

-----