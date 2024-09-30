# Databricks notebook source
op= dbutils.notebook.run(path='../setup/adls_connection_func', #notebook path to call
                     timeout_seconds=60,
                     arguments={'storage_name':'dludemycourse','container_name':'raw'})

# COMMAND ----------

if eval(op)["Value"] != "success":
    raise Exception('Something failed while mounting')

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

circuit_schema = StructType([StructField("circuitId", IntegerType(), False),
                             StructField("circuitRef", StringType(), True),
                             StructField("name", StringType(), True),
                             StructField("location", StringType(), True),
                             StructField("country", StringType(), True),
                             StructField("lat", DoubleType(), True),
                             StructField("lng", DoubleType(), True),
                             StructField("alt", IntegerType(), True),
                             StructField("url", StringType(), True)])

# COMMAND ----------

circuit_data = spark.read.csv("/mnt/dludemycourse/raw/circuits.csv", header=True, schema=circuit_schema)

# COMMAND ----------

circuit_selected_data = circuit_data.select("circuitId","circuitRef", "name", "location", "country","lat", "lng","alt")

# COMMAND ----------

circuit_renamed_data = circuit_selected_data.withColumnRenamed("circuitId", "circuit_id").withColumnRenamed("circuitRef", "circuit_ref").withColumnRenamed("lat", "latitude").withColumnRenamed("lng", "longitude").withColumnRenamed("alt", "altitude")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Adding ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
circuits_final_df = circuit_renamed_data.withColumn("ingestion_date", current_timestamp()).withColumn("env", lit("Dev"))

# COMMAND ----------

op= dbutils.notebook.run(path='../setup/adls_connection_func', #notebook path to call
                     timeout_seconds=60,
                     arguments={'storage_name':'dludemycourse','container_name':'processed'})

# COMMAND ----------

if eval(op)["Value"] != "success":
    raise Exception('Something failed while mounting')

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/dludemycourse/processed/circuits")

# COMMAND ----------

race_schema = StructType([StructField("raceId", IntegerType(), False),
                             StructField("year", IntegerType(), True),
                             StructField("round", IntegerType(), True),
                             StructField("circuitId", IntegerType(), True),
                             StructField("name", StringType(), True),
                             StructField("date", StringType(), True),
                             StructField("time", StringType(), True),
                             StructField("url", StringType(), True)])

# COMMAND ----------

race_data = spark.read.csv("/mnt/dludemycourse/raw/races.csv", header=True, schema=race_schema)

# COMMAND ----------

race_selected_data = race_data.select("raceId","year","round","circuitId","name","date","time")

# COMMAND ----------

race_renamed_data = race_selected_data.withColumnRenamed("raceId", "race_id").withColumnRenamed("year", "race_year").withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, concat,lit
race_final_df = race_renamed_data.withColumn("race_timestamp", to_timestamp(concat(col("date"),lit(" "),col("time")))) \
.withColumn("ingestion_date",current_timestamp()).drop("date").drop("time")

# COMMAND ----------

race_final_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/dludemycourse/processed/races")

# COMMAND ----------

race_final_df.write.mode("overwrite").partitionBy("race_year").format("delta").save("/mnt/
