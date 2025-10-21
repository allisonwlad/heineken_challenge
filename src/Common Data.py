# Databricks notebook source
# MAGIC %md
# MAGIC <h1> Prepara os dados da stage, define data types, cruza os dados de CEP enriquece uma flat table com as informações necessárias para o modelo dimensional
# MAGIC

# COMMAND ----------

sales = spark.read.table('xyz.stg.tab_sales')
display(sales)

# COMMAND ----------

# MAGIC %md
# MAGIC <h3> Formata o campo data_doc para o tipo date correto, formata materialPackaging para o tipo INT

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, year
sales = sales

# Detecta o padrão e converte de acordo
sales = sales.withColumn(
    "data_formatada",
    F.when(
        F.col("data_doc").rlike(r"^\d{4}-\d{2}-\d{2}$"),  # formato YYYY-MM-DD
        F.to_date("data_doc", "yyyy-MM-dd")
    ).when(
        F.col("data_doc").rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"),  # formato D/M/YYYY
        F.to_date("data_doc", "d/M/yyyy")
    ).otherwise(None)
)

sales = sales.withColumn("materialPackaging_cast", col("materialPackaging").cast("int"))
sales = sales.withColumn("year", year(sales["data_formatada"]))
display(sales)




# COMMAND ----------

# MAGIC %md
# MAGIC <h3> Carrega os dados de metas
# MAGIC

# COMMAND ----------

metas = spark.read.table('xyz.stg.tab_metas')
display(metas)


# COMMAND ----------

# MAGIC %md
# MAGIC <h3> Cruza as metas pelo produto e pela meta e pelo ano da meta

# COMMAND ----------

flat_join = sales.join(metas, (col("brand_desc") == col("Marca")) & (col("year") == col("Ano")), "left")
display(flat_join)

# COMMAND ----------

# MAGIC %md
# MAGIC <h3>Função de busca da cidade/estado pelo CEP do cliente

# COMMAND ----------

import pandas as pd
import requests

def buscar_endereco_em_lote(ceps):
    resultados = []
    for cep in ceps:
        cep = ''.join(filter(str.isdigit, str(cep)))
        try:
            r = requests.get(f"https://viacep.com.br/ws/{cep}/json/", timeout=2)
            if r.status_code == 200:
                data = r.json()
                resultados.append({
                    "CEP_cliente": cep,
                    "cidade": data.get("localidade"),
                    "estado": data.get("uf")
                })
        except:
            resultados.append({"CEP_cliente": cep, "cidade": None, "estado": None})
    return pd.DataFrame(resultados)

# COMMAND ----------

# MAGIC %md
# MAGIC <h3> Busca os dados de estado e cidade pelo CEP do cliente e adiciona ao dataframe
# MAGIC

# COMMAND ----------

ceps_unicos = flat_join.select("CEP_cliente").distinct().toPandas()
df_end = buscar_endereco_em_lote(ceps_unicos["CEP_cliente"])
df_end_spark = spark.createDataFrame(df_end)

df_final = flat_join.join(df_end_spark, on="CEP_cliente", how="left")

display(df_final)
df_final.write.mode("overwrite").saveAsTable('xyz.com.tab_sales_flat')