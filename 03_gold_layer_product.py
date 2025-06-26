# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Gold: Criação de Produtos de Dados
# MAGIC
# MAGIC Este notebook é responsável por criar produtos de dados agregados e otimizados para análise, a partir da camada Silver.

# COMMAND ----------

from pyspark.sql.functions import sum, count, countDistinct, avg, dayofmonth, month, year, date_trunc, desc, col

# COMMAND ----------

# Configurações de Paths e Nomes de Tabela
silver_table_path = "dbfs:/user/hive/warehouse/ecommerce_data/silver/online_retail_silver"

# Produtos de dados (tabelas Gold)
gold_sales_by_product_path = "dbfs:/user/hive/warehouse/ecommerce_data/gold/sales_by_product"
gold_sales_by_product_name = "sales_by_product"

gold_daily_sales_path = "dbfs:/user/hive/warehouse/ecommerce_data/gold/daily_sales"
gold_daily_sales_name = "daily_sales"

gold_top_customers_path = "dbfs:/user/hive/warehouse/ecommerce_data/gold/top_customers"
gold_top_customers_name = "top_customers"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura da Camada Silver
# MAGIC
# MAGIC Vamos ler os dados da tabela Delta da camada Silver.

# COMMAND ----------

df_silver = spark.read.format("delta").load(silver_table_path)
df_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Produto de Dados 1: Vendas Totais por Produto
# MAGIC
# MAGIC Agregação para mostrar a quantidade total vendida e a receita total por produto.

# COMMAND ----------

df_sales_by_product = df_silver.groupBy("stock_code", "description") \
  .agg(
    sum("quantity").alias("total_quantity_sold"),
    sum("total_price").alias("total_revenue")
  ) \
  .orderBy(desc("total_revenue"))

df_sales_by_product.limit(10).display()

# Salvar na camada Gold
df_sales_by_product.write.format("delta") \
  .mode("overwrite") \
  .option("path", gold_sales_by_product_path) \
  .saveAsTable(gold_sales_by_product_name)

print(f"Produto de dados 'Vendas por Produto' salvo em: {gold_sales_by_product_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Produto de Dados 2: Vendas Diárias
# MAGIC
# MAGIC Agregação para mostrar a receita total e o número de transações por dia.

# COMMAND ----------

df_daily_sales = df_silver.withColumn("sale_date", date_trunc("day", col("invoice_date"))) \
  .groupBy("sale_date") \
  .agg(
    sum("total_price").alias("daily_revenue"),
    countDistinct("invoice_no").alias("daily_transactions_count"),
    countDistinct("customer_id").alias("daily_unique_customers")
  ) \
  .orderBy("sale_date")

df_daily_sales.limit(10).display()

# Salvar na camada Gold
df_daily_sales.write.format("delta") \
  .mode("overwrite") \
  .option("path", gold_daily_sales_path) \
  .saveAsTable(gold_daily_sales_name)

print(f"Produto de dados 'Vendas Diárias' salvo em: {gold_daily_sales_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Produto de Dados 3: Top 10 Clientes por Receita
# MAGIC
# MAGIC Agregação para identificar os clientes que mais geraram receita.

# COMMAND ----------

df_top_customers = df_silver.groupBy("customer_id") \
  .agg(
    sum("total_price").alias("total_spent"),
    countDistinct("invoice_no").alias("total_invoices")
  ) \
  .orderBy(desc("total_spent")) \
  .limit(10)

df_top_customers.display()

# Salvar na camada Gold
df_top_customers.write.format("delta") \
  .mode("overwrite") \
  .option("path", gold_top_customers_path) \
  .saveAsTable(gold_top_customers_name)

print(f"Produto de dados 'Top Clientes' salvo em: {gold_top_customers_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação e Visualização (Opcional)
# MAGIC
# MAGIC Você pode usar os recursos de visualização do Databricks diretamente nos DataFrames ou tabelas Gold.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exemplo de consulta e visualização de vendas por produto
# MAGIC SELECT description, total_revenue FROM sales_by_product ORDER BY total_revenue DESC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exemplo de consulta e visualização de vendas diárias
# MAGIC SELECT sale_date, daily_revenue FROM daily_sales ORDER BY sale_date;