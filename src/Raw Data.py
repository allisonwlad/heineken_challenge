# Databricks notebook source
# MAGIC %md
# MAGIC <h1>Volume and upload data files

# COMMAND ----------

dbutils.fs.ls('/Volumes/xyz/stg/input_files')

# COMMAND ----------

# MAGIC %md
# MAGIC <h1> Lê o arquivo de entrada de vendas, separa os dominios em dimensões e já trata os dados da API de CEP

# COMMAND ----------

# MAGIC %md
# MAGIC <h3> Insere os dados de sales e metas em uma stage layer

# COMMAND ----------

import pandas as pd
# Caminho do arquivo (você pode subir o .xlsx pelo menu "Data" → "Upload File")
excel_path = "/Volumes/xyz/stg/input_files/sales.xlsx"
# Lê o Excel com pandas - versão free edition do Databricks limita a utilização de bibliotecas instaladas por isso o uso do pandas
pdf = pd.read_excel(excel_path)
pdf.columns = [col.replace(' ', '_').replace('ç', 'c').replace('ã', 'a') for col in pdf.columns]

# Converte para DataFrame Spark
df = spark.createDataFrame(pdf)
# Mostra os dados
display(df)
df.write.format("delta").mode("overwrite").saveAsTable("xyz.stg.tab_sales")


# COMMAND ----------

metas = spark.read.csv("/Volumes/xyz/stg/input_files/metas por marca.csv", header=True, sep=';')
display(metas)
metas.write.format("delta").mode("overwrite").saveAsTable("xyz.stg.tab_metas")
