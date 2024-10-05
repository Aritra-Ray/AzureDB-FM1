# Databricks notebook source
driver_rank = spark.read.parquet("/mnt/dludemycourse/presentation/drivers_rank")
spark.sql("drop table if exists team_rank")
driver_rank.write.mode("overwrite").saveAsTable("drivers_rank")

# COMMAND ----------

team_rank = spark.read.parquet("/mnt/dludemycourse/presentation/team_rank")
spark.sql("drop table if exists team_rank")
team_rank.write.mode("overwrite").saveAsTable("team_rank")
