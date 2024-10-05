# Databricks notebook source
constructors_schema = 'constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING'

# COMMAND ----------

op= dbutils.notebook.run(path='../setup/adls_connection_func', #notebook path to call
                     timeout_seconds=60,
                     arguments={'storage_name':'dludemycourse','container_name':'raw'})

# COMMAND ----------

if eval(op)["Value"] != "success":
    raise Exception('Something failed while mounting')

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json("/mnt/dludemycourse/raw/constructors.json")

# COMMAND ----------

constructors_removed_df = constructors_df.drop("url")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

constructors_final_df = constructors_removed_df.withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("constructorRef", "constructor_ref").withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

constructors_final_df.write.mode("overwrite").parquet("/mnt/dludemycourse/processed/constructors")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,DateType,IntegerType

# COMMAND ----------

nameschema = StructType([StructField("forename",StringType())
                         ,StructField("surname",StringType())])

# COMMAND ----------

drivers_schema = StructType([StructField("driverId",IntegerType(),False)
                             ,StructField("driverRef",StringType(),True)
                             ,StructField("number",IntegerType(),True)
                             ,StructField("code",StringType(),True)
                             ,StructField("name",nameschema)
                             ,StructField("dob",DateType(),True)
                             ,StructField("nationality",StringType(),True)
                             ,StructField("url",StringType(),True)])

# COMMAND ----------

drivers_data = spark.read.schema(drivers_schema).json("/mnt/dludemycourse/raw/drivers.json")

# COMMAND ----------

from pyspark.sql.functions import col,lit,concat,current_timestamp
drivers_modified_data = drivers_data.withColumnRenamed("driverId","driver_id")\
                           .withColumnRenamed("driverRef","driver_ref")\
                           .withColumn("ingestion_date",current_timestamp())\
                           .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))  
                           

# COMMAND ----------

drivers_final_data = drivers_modified_data.drop("url")

# COMMAND ----------

drivers_final_data.write.mode("overwrite").parquet('/mnt/dludemycourse/processed/drivers')

# COMMAND ----------

results_schema = 'resultId INT, raceId INT,  driverId INT, constructorId INT , number INT,grid INT, position INT, positionText INT, positionOrder INT, points INT, laps INT, time STRING, milliseconds INT, fastestLap INT, rank INT, fastestLapTime STRING, fastestLapSpeed DOUBLE, statusId INT'

# COMMAND ----------

results_data = spark.read.schema(results_schema).json('/mnt/dludemycourse/raw/results.json')

# COMMAND ----------

results_final_data = results_data.withColumnRenamed("resultId","result_id")\
                                .withColumnRenamed("raceId","race_id")\
                                 .withColumnRenamed("courseId","course_id")\
                                 .withColumnRenamed("driverId","driver_id")\
                                 .withColumnRenamed("constructorId","constructor_id")\
                                 .withColumnRenamed("positionText","position_text")\
                                 .withColumnRenamed("positionOrder","position_order")\
                                 .withColumnRenamed("fastestLap","fastest_lap")\
                                 .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                                 .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                                .withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

results_final_data.drop("statusid").write.mode("overwrite").partitionBy("race_id").parquet('/mnt/dludemycourse/processed/results')

# COMMAND ----------

pitsstop_schema = 'raceId INT, driverId INT,  stop STRING, lap INT , time STRING,duration STRING, milliseconds INT'

# COMMAND ----------

pitsstop_data = spark.read.schema(pitsstop_schema).option("multiline", True).json('/mnt/dludemycourse/raw/pit_stops.json')

# COMMAND ----------

pitsstop_final_data = pitsstop_data.withColumnRenamed('raceId','race_id') \
                        .withColumnRenamed('driverId','driver_id') \
                        .withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

pitsstop_final_data.write.mode("overwrite").parquet('/mnt/dludemycourse/processed/pit_stops/')

# COMMAND ----------

qualify_schema = 'qualifyId INT, raceId INT, driverId INT, constructorId INT, number INT, position INT, q1 STRING, q2 STRING, q3 STRING'

# COMMAND ----------

qualify_data = spark.read.schema(qualify_schema).option("multiline",True).json('/mnt/dludemycourse/raw/qualifying/')

# COMMAND ----------

qualify_final_data = qualify_data.withColumnRenamed('qualifyingId','qualifying_id')\
                                    .withColumnRenamed('raceId','race_id')\
                                    .withColumnRenamed('driverId','driver_id')\
                                .withColumnRenamed('constructorId','constructor_id')\
                                    .withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

qualify_final_data.write.mode("overwrite").parquet('/mnt/dludemycourse/processed/qualifying/')
