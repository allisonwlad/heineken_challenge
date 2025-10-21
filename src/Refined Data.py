# Databricks notebook source
# MAGIC %md
# MAGIC <h1> Load flat table (sales, address)

# COMMAND ----------

flat = spark.read.table('xyz.com.tab_sales_flat')
display(flat)

# COMMAND ----------

# MAGIC %md
# MAGIC <h3> Load Calendar Dimension

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import date, timedelta
from pyspark.sql.functions import monotonically_increasing_id

# Defina o intervalo do calendário
data_inicial = date(2018, 1, 1)
data_final   = date(2030, 12, 31)

# Gera a lista de datas no Python
dias = (data_final - data_inicial).days + 1
lista_datas = [(data_inicial + timedelta(days=i),) for i in range(dias)]

# Cria DataFrame base
df_calendario = spark.createDataFrame(lista_datas, ["date"])

# Adiciona colunas úteis
df_calendario = (
    df_calendario
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("week_day_name", F.date_format("date", "EEEE"))
    .withColumn("day_of_week", F.dayofweek("date"))  # 1=Sunday, 7=Saturday
    .withColumn("week_year", F.weekofyear("date"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn(
        "weekend",
        F.when(F.col("day_of_week").isin(1, 7), F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn("year_month", F.date_format("date", "yyyy-MM"))
)

display(df_calendario)
df_calendario.write.format('delta').mode('overwrite').saveAsTable('xyz.ref.dim_calender')


# COMMAND ----------

# MAGIC %md
# MAGIC <h3> Load Customer dimension

# COMMAND ----------

customer = flat[['Código_do_Cliente', 'CEP_cliente']].dropDuplicates().withColumnRenamed('Código_do_Cliente', 'customer_id').withColumnRenamed('CEP_cliente', 'zip_code').withColumn('sk_customer',monotonically_increasing_id())
customer.write.mode('overwrite').option('overwriteschema', 'true').saveAsTable('xyz.ref.dim_customer')

# COMMAND ----------

# MAGIC %md
# MAGIC <h3> Load Address dimension, this dimensional has many categories (in this case adopt snow flake schema)

# COMMAND ----------

address = flat[['CEP_cliente', 'cidade', 'estado']].dropDuplicates().withColumnRenamed('CEP_cliente', 'zip_code').withColumnRenamed('cidade', 'city').withColumnRenamed('estado', 'state').fillna('N/A').withColumn('sk_address',monotonically_increasing_id())
address.write.mode('overwrite').option('overwriteschema', 'true').saveAsTable('xyz.ref.dim_address')

# COMMAND ----------

# MAGIC %md
# MAGIC <h3>Load Material dimensional, material_type have 4 categories (adopt star schema without snow flake)

# COMMAND ----------

material = flat[['cod_material', 'b2b_status', 'materialPackaging', 'material']].dropDuplicates().fillna('N/A').withColumnRenamed('cod_material','material_id').withColumnRenamed('materialPackaging', 'material_packaging').withColumnRenamed('material', 'material_type').withColumn('sk_material',monotonically_increasing_id())
display(material)
material.write.format('delta').mode('overwrite').option('overwriteschema', 'true').saveAsTable('xyz.ref.dim_material')

# COMMAND ----------

# MAGIC %md
# MAGIC <h3>Load Brand Dimension

# COMMAND ----------

brand = flat[['brand', 'brand_desc']].dropDuplicates().fillna('N/A').withColumnRenamed('brand', 'brand_id').withColumnRenamed('brand_desc', 'brand_desc').withColumn('sk_brand',monotonically_increasing_id())
brand.write.format('delta').mode('overwrite').option('overwriteschema', 'true').saveAsTable('xyz.ref.dim_brand')

# COMMAND ----------

# MAGIC %md
# MAGIC <h3> Load Goals Dimension

# COMMAND ----------

metas = spark.read.table('xyz.stg.tab_metas').withColumn('sk_meta', monotonically_increasing_id())
display(metas)
metas.write.format('delta').mode('overwrite').option('overwriteschema', 'true').saveAsTable('xyz.ref.dim_metas')

# COMMAND ----------

# MAGIC %md
# MAGIC <h4> Adjust for years or Brands without goal

# COMMAND ----------

# MAGIC %sql
# MAGIC update xyz.ref.dim_metas set Ano=-1, Meta=0 where sk_meta=-1

# COMMAND ----------

# MAGIC %md
# MAGIC <h3> Create and Load fact's (SALES, SALESS AGG WITH SHARE AND AVG 3M 6M)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE xyz.ref.fact_sales (
# MAGIC   sk_customer BIGINT,
# MAGIC   sk_brand BIGINT,
# MAGIC   date DATE,
# MAGIC   sk_material BIGINT,
# MAGIC   sk_meta BIGINT,
# MAGIC   amount double
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE xyz.ref.fact_sales_agg(
# MAGIC   customer_id BIGINT,
# MAGIC   dt_ultima_compra DATE,
# MAGIC   vl_media_valor_3m double,
# MAGIC   vl_media_valor_6m double,
# MAGIC   vl_share_b2b_3m double,
# MAGIC   vl_share_b2b_6m double
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE xyz.ref.fact_sales_meta (
# MAGIC   sk_customer bigint,
# MAGIC   sk_brand bigint,
# MAGIC   ano int,
# MAGIC   volume_cliente double,
# MAGIC   volume_total_marca double,
# MAGIC   participacao double,
# MAGIC   meta double,
# MAGIC   meta_cliente double
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC <h4> Fato sales (soma dos valores por dimensões)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO xyz.ref.fact_sales 
# MAGIC SELECT c.sk_customer,
# MAGIC        b.sk_brand,
# MAGIC        cal.date,
# MAGIC        m.sk_material,
# MAGIC        CASE
# MAGIC          WHEN try_cast(met.sk_Meta AS BIGINT) IS NULL THEN -1
# MAGIC          ELSE try_cast(met.sk_Meta AS BIGINT)
# MAGIC        END AS sk_meta,
# MAGIC        sum(f.valor)
# MAGIC FROM xyz.com.tab_sales_flat f
# MAGIC inner join xyz.ref.dim_customer c on f.`Código_do_Cliente`=c.customer_id
# MAGIC inner join xyz.ref.dim_brand b on f.brand=b.brand_id
# MAGIC inner join xyz.ref.dim_material m on f.cod_material=m.material_id
# MAGIC inner join xyz.ref.dim_calender cal on f.data_formatada=cal.date
# MAGIC left join xyz.ref.dim_metas met on year(f.data_formatada)=met.Ano and f.brand_desc=met.Marca
# MAGIC group by c.sk_customer,
# MAGIC        b.sk_brand,
# MAGIC        cal.date,
# MAGIC        m.sk_material,
# MAGIC        CASE
# MAGIC          WHEN try_cast(met.sk_meta AS BIGINT) IS NULL THEN -1
# MAGIC          ELSE try_cast(met.sk_meta AS BIGINT)
# MAGIC        END
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <h4> Calcula Média dos últimos 3 e  6 meses e share b2b pelo mesmo período

# COMMAND ----------

# MAGIC %sql
# MAGIC -- pega a última data de compra por cliente
# MAGIC WITH ultima_compra AS (
# MAGIC   SELECT 
# MAGIC     `Código_do_Cliente` as cliente_id,
# MAGIC     MAX(data_formatada) AS ultima_compra
# MAGIC   FROM xyz.com.tab_sales_flat
# MAGIC   GROUP BY `Código_do_Cliente`
# MAGIC ),
# MAGIC
# MAGIC -- Junta e define janelas de 3 e 6 meses
# MAGIC compras_com_intervalo AS (
# MAGIC   SELECT
# MAGIC     f.`Código_do_Cliente` as cliente_id,
# MAGIC     f.data_formatada as data_compra,
# MAGIC     f.valor,
# MAGIC     f.b2b_status,
# MAGIC     u.ultima_compra,
# MAGIC     ADD_MONTHS(u.ultima_compra, -3) AS data_3m,
# MAGIC     ADD_MONTHS(u.ultima_compra, -6) AS data_6m
# MAGIC   FROM xyz.com.tab_sales_flat f
# MAGIC   JOIN ultima_compra u
# MAGIC     ON f.`Código_do_Cliente` = u.cliente_id
# MAGIC ),
# MAGIC
# MAGIC -- Calcula faturamento total e B2B por cliente e mês
# MAGIC faturamento_mensal AS (
# MAGIC   SELECT
# MAGIC     cliente_id,
# MAGIC     DATE_TRUNC('month', data_compra) AS mes,
# MAGIC     SUM(CASE WHEN b2b_status THEN valor ELSE 0 END) AS faturamento_b2b,
# MAGIC     SUM(valor) AS faturamento_total,
# MAGIC     AVG(valor) AS media_mensal_valor,
# MAGIC     MAX(ultima_compra) AS ultima_compra,
# MAGIC     MAX(data_3m) AS data_3m,
# MAGIC     MAX(data_6m) AS data_6m
# MAGIC   FROM compras_com_intervalo
# MAGIC   GROUP BY cliente_id, DATE_TRUNC('month', data_compra)
# MAGIC ),
# MAGIC
# MAGIC -- Calcula o Share B2B mensal
# MAGIC share_mensal AS (
# MAGIC   SELECT
# MAGIC     cliente_id,
# MAGIC     mes,
# MAGIC     faturamento_b2b,
# MAGIC     faturamento_total,
# MAGIC     media_mensal_valor,
# MAGIC     CASE 
# MAGIC       WHEN faturamento_total > 0 THEN faturamento_b2b / faturamento_total 
# MAGIC       ELSE 0 
# MAGIC     END AS share_b2b,
# MAGIC     ultima_compra,
# MAGIC     data_3m,
# MAGIC     data_6m
# MAGIC   FROM faturamento_mensal
# MAGIC )
# MAGIC
# MAGIC --Agrega tudo — médias e shares nos últimos 3 e 6 meses
# MAGIC INSERT INTO xyz.ref.fact_sales_agg 
# MAGIC SELECT
# MAGIC   cliente_id,
# MAGIC   MAX(ultima_compra) AS ultima_compra,
# MAGIC
# MAGIC   ROUND(AVG(CASE WHEN mes > data_3m AND mes <= ultima_compra THEN media_mensal_valor END), 2) AS media_valor_3m,
# MAGIC   ROUND(AVG(CASE WHEN mes > data_6m AND mes <= ultima_compra THEN media_mensal_valor END), 2) AS media_valor_6m,
# MAGIC
# MAGIC   ROUND(AVG(CASE WHEN mes > data_3m AND mes <= ultima_compra THEN share_b2b END), 4) AS share_b2b_3m,
# MAGIC   ROUND(AVG(CASE WHEN mes > data_6m AND mes <= ultima_compra THEN share_b2b END), 4) AS share_b2b_6m
# MAGIC
# MAGIC FROM share_mensal
# MAGIC GROUP BY cliente_id
# MAGIC ORDER BY cliente_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Soma o volume total de vendas por marca e cliente
# MAGIC WITH vendas_cliente_marca AS (
# MAGIC   SELECT
# MAGIC     c.sk_customer as cliente_id,
# MAGIC     B.sk_brand as sk_brand,
# MAGIC     B.brand_desc as marca,
# MAGIC     YEAR(cal.date) as ano,
# MAGIC     SUM(f.amount) AS volume_cliente
# MAGIC   FROM xyz.ref.fact_sales f
# MAGIC   INNER JOIN xyz.ref.dim_customer c
# MAGIC     ON f.sk_customer = c.sk_customer
# MAGIC   INNER JOIN xyz.ref.dim_brand b
# MAGIC     ON f.sk_customer = b.sk_brand
# MAGIC   INNER JOIN xyz.ref.dim_calender cal 
# MAGIC     on f.date=cal.date
# MAGIC   LEFT JOIN xyz.ref.dim_metas meta 
# MAGIC     on b.brand_desc=meta.Marca and year(cal.date)=meta.Ano
# MAGIC   WHERE meta.sk_meta IS NOT NULL
# MAGIC   GROUP BY c.sk_customer, B.sk_brand, b.brand_desc, YEAR(cal.date)
# MAGIC ),
# MAGIC
# MAGIC -- calcula o volume total por marca
# MAGIC vendas_marca AS (
# MAGIC   SELECT
# MAGIC     marca,
# MAGIC     sk_brand,
# MAGIC     ano, 
# MAGIC     SUM(volume_cliente) AS volume_total_marca
# MAGIC   FROM vendas_cliente_marca
# MAGIC   GROUP BY marca, ano, sk_brand
# MAGIC ),
# MAGIC
# MAGIC -- Junta tudo e calcula a participação de cada cliente
# MAGIC participacao_cliente AS (
# MAGIC   SELECT
# MAGIC     v.cliente_id,
# MAGIC     v.marca,
# MAGIC     v.sk_brand,
# MAGIC     v.ano,
# MAGIC     v.volume_cliente,
# MAGIC     m.volume_total_marca,
# MAGIC     ROUND(v.volume_cliente / m.volume_total_marca, 6) AS participacao
# MAGIC   FROM vendas_cliente_marca v
# MAGIC   JOIN vendas_marca m
# MAGIC     ON v.marca = m.marca
# MAGIC ),
# MAGIC
# MAGIC -- Junta com a meta de marca e distribui proporcionalmente
# MAGIC meta_cliente AS (
# MAGIC   SELECT
# MAGIC     p.cliente_id,
# MAGIC     p.marca,
# MAGIC     p.sk_brand,
# MAGIC     p.volume_cliente,
# MAGIC     p.volume_total_marca,
# MAGIC     p.participacao,
# MAGIC     p.ano,
# MAGIC     meta.meta,
# MAGIC     ROUND(p.participacao * meta.meta, 2) AS meta_cliente
# MAGIC   FROM participacao_cliente p
# MAGIC   JOIN xyz.ref.dim_metas meta
# MAGIC     ON p.marca = meta.Marca
# MAGIC )
# MAGIC
# MAGIC -- Resultado final: meta distribuída por cliente
# MAGIC INSERT INTO xyz.ref.fact_sales_meta
# MAGIC SELECT
# MAGIC   cliente_id,
# MAGIC   sk_brand,
# MAGIC   ano,
# MAGIC   volume_cliente,
# MAGIC   volume_total_marca,
# MAGIC   participacao,
# MAGIC   meta,
# MAGIC   meta_cliente
# MAGIC FROM meta_cliente
# MAGIC ORDER BY marca, cliente_id, ano;
# MAGIC