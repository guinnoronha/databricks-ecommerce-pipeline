# MAGIC %md
# MAGIC # Camada Bronze: Ingestão de Dados Brutos
# MAGIC
# MAGIC Este notebook é responsável por ingerir os dados brutos do CSV para o formato Delta Lake, criando a camada Bronze.

# COMMAND ----------

# Configurações de Paths
raw_data_path = "dbfs:/FileStore/datasets/ecommerce_data/raw/data.csv"
bronze_table_path = "dbfs:/user/hive/warehouse/ecommerce_data/bronze/online_retail_bronze"
bronze_table_name = "online_retail_bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura dos Dados Brutos (CSV)
# MAGIC
# MAGIC Vamos ler o arquivo CSV. É importante inferir o esquema e usar a primeira linha como cabeçalho.

# COMMAND ----------

# Leitura do CSV
# Ajuste o delimitador e encoding se necessário. Para este dataset, comma e utf-8 são comuns.
df_raw = spark.read.format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .option("delimiter", ",") \
  .load(raw_data_path)

# Exibir o esquema inferido e algumas linhas para verificação
df_raw.printSchema()
df_raw.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escrita dos Dados para Delta Lake (Camada Bronze)
# MAGIC
# MAGIC Vamos salvar os dados lidos no formato Delta Lake.
# MAGIC
# MAGIC Usamos o modo `overwrite` para a primeira execução. Em um cenário real de streaming, usaríamos `append` com Auto Loader.

# COMMAND ----------

# Salvar os dados no formato Delta Lake
df_raw.write.format("delta") \
  .mode("overwrite") \
  .option("path", bronze_table_path) \
  .saveAsTable(bronze_table_name)

print(f"Dados brutos salvos com sucesso na tabela Delta: {bronze_table_name} em {bronze_table_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação da Tabela Bronze
# MAGIC
# MAGIC Vamos ler a tabela Delta recém-criada para verificar se a ingestão foi bem-sucedida.

# COMMAND ----------

# Leitura da tabela Delta para verificação
df_bronze = spark.read.format("delta").load(bronze_table_path)
df_bronze.printSchema()
df_bronze.limit(5).display()
