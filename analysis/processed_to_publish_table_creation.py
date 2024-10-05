# Databricks notebook source
op= dbutils.notebook.run(path='../setup/adls_connection_func', #notebook path to call
                     timeout_seconds=60,
                     arguments={'storage_name':'dludemycourse','container_name':'presentation'})

# COMMAND ----------

if eval(op)["Value"] != "success":
    raise Exception('Something failed while mounting')

# COMMAND ----------

df = spark.read.parquet("/mnt/dludemycourse/processed/circuits/")
spark.sql('drop table if exists circuits')
df.write.saveAsTable ("circuits")

# COMMAND ----------

df = spark.read.parquet("/mnt/dludemycourse/processed/constructors/")
spark.sql('drop table if exists constructors')
df.write.saveAsTable ("constructors")

# COMMAND ----------

df = spark.read.parquet("/mnt/dludemycourse/processed/drivers/")
spark.sql('drop table if exists drivers')
df.write.saveAsTable ("drivers")

# COMMAND ----------

df = spark.read.parquet("/mnt/dludemycourse/processed/laps_time/")
spark.sql('drop table if exists laps_time')
df.write.saveAsTable ("laps_time")

# COMMAND ----------

df = spark.read.parquet("/mnt/dludemycourse/processed/races/")
spark.sql('drop table if exists races')
df.write.saveAsTable ("races")

# COMMAND ----------

df = spark.read.parquet("/mnt/dludemycourse/processed/results/")
spark.sql('drop table if exists results')
df.write.saveAsTable ("results")
