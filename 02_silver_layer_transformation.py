# MAGIC %md
# MAGIC # Camada Silver: Limpeza e Transformação
# MAGIC
# MAGIC Este notebook é responsável por limpar, padronizar e enriquecer os dados da camada Bronze, criando a camada Silver.

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, lit, when, trim, lower
from pyspark.sql.types import IntegerType, DoubleType

# COMMAND ----------

# Configurações de Paths e Nomes de Tabela
bronze_table_path = "dbfs:/user/hive/warehouse/ecommerce_data/bronze/online_retail_bronze"
silver_table_path = "dbfs:/user/hive/warehouse/ecommerce_data/silver/online_retail_silver"
silver_table_name = "online_retail_silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura da Camada Bronze
# MAGIC
# MAGIC Vamos ler os dados da tabela Delta da camada Bronze.

# COMMAND ----------

df_bronze = spark.read.format("delta").load(bronze_table_path)
df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformações e Limpeza
# MAGIC
# MAGIC 1.  **Renomear Colunas:** Padronizar nomes de colunas (ex: sem espaços, camelCase).
# MAGIC 2.  **Tratar Valores Nulos:** Remover linhas com `InvoiceNo` ou `CustomerID` nulos (críticos para análise).
# MAGIC 3.  **Conversão de Tipos:** Garantir que `Quantity`, `UnitPrice` e `CustomerID` sejam numéricos, e `InvoiceDate` seja um timestamp.
# MAGIC 4.  **Remover Duplicatas:** Com base em uma combinação de colunas que identifique uma transação única.
# MAGIC 5.  **Filtrar Dados Inválidos:** Remover quantidades ou preços unitários negativos/zero.
# MAGIC 6.  **Criar Colunas Derivadas:** `TotalPrice` (Quantity * UnitPrice).

# COMMAND ----------

df_silver = df_bronze.select(
    col("InvoiceNo").alias("invoice_no"),
    to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm").alias("invoice_date"), # Ajuste o formato da data conforme seu CSV
    col("StockCode").alias("stock_code"),
    col("Description").alias("description"),
    col("Quantity").cast(IntegerType()).alias("quantity"),
    col("UnitPrice").cast(DoubleType()).alias("unit_price"),
    col("CustomerID").cast(IntegerType()).alias("customer_id"),
    col("Country").alias("country")
)

# Tratar valores nulos em colunas críticas
df_silver = df_silver.na.drop(subset=["invoice_no", "customer_id", "quantity", "unit_price"])

# Filtrar quantidades e preços unitários válidos (positivos)
df_silver = df_silver.filter((col("quantity") > 0) & (col("unit_price") > 0))

# Remover linhas duplicadas (considerando que InvoiceNo + StockCode + CustomerID + Quantity + UnitPrice + InvoiceDate pode ser um identificador único para uma linha de item)
df_silver = df_silver.dropDuplicates(["invoice_no", "stock_code", "customer_id", "quantity", "unit_price", "invoice_date"])

# Limpar e padronizar a coluna Description (remover espaços extras, converter para minúsculas)
df_silver = df_silver.withColumn("description", trim(lower(col("description"))))

# Criar a coluna TotalPrice
df_silver = df_silver.withColumn("total_price", col("quantity") * col("unit_price"))

# Exibir o esquema e algumas linhas da camada Silver
df_silver.printSchema()
df_silver.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escrita dos Dados para Delta Lake (Camada Silver)
# MAGIC
# MAGIC Salvar os dados limpos e transformados no formato Delta Lake.

# COMMAND ----------

df_silver.write.format("delta") \
  .mode("overwrite") \
  .option("path", silver_table_path) \
  .saveAsTable(silver_table_name)

print(f"Dados limpos e transformados salvos com sucesso na tabela Delta: {silver_table_name} em {silver_table_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação da Tabela Silver
# MAGIC
# MAGIC Vamos ler a tabela Delta recém-criada para verificar a camada Silver.

# COMMAND ----------

df_silver_check = spark.read.format("delta").load(silver_table_path)
df_silver_check.printSchema()
df_silver_check.limit(5).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Você também pode consultar a tabela usando SQL
# MAGIC SELECT * FROM online_retail_silver LIMIT 10;