# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS calculated_race_results
# MAGIC               (
# MAGIC               race_year INT,
# MAGIC               team_name STRING,
# MAGIC               driver_id INT,
# MAGIC               driver_name STRING,
# MAGIC               race_id INT,
# MAGIC               position INT,
# MAGIC               points INT,
# MAGIC               calculated_points INT,
# MAGIC               created_date TIMESTAMP,
# MAGIC               updated_date TIMESTAMP
# MAGIC               )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW race_result_updated
# MAGIC               AS
# MAGIC               SELECT ra.race_year,
# MAGIC                      c.name AS team_name,
# MAGIC                      d.driver_id,
# MAGIC                      d.name AS driver_name,
# MAGIC                      ra.race_id,
# MAGIC                      re.position,
# MAGIC                      re.points,
# MAGIC                      11 - re.position AS calculated_points
# MAGIC                 FROM results  as re
# MAGIC                 JOIN drivers as d ON (re.driver_id = d.driver_id)
# MAGIC                 JOIN constructors as c ON (re.constructor_id = c.constructor_id)
# MAGIC                 JOIN races as ra ON (re.race_id = ra.race_id)
# MAGIC                WHERE re.position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO calculated_race_results tgt
# MAGIC               USING race_result_updated upd
# MAGIC               ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
# MAGIC               WHEN MATCHED THEN
# MAGIC                 UPDATE SET tgt.position = upd.position,
# MAGIC                            tgt.points = upd.points,
# MAGIC                            tgt.calculated_points = upd.calculated_points,
# MAGIC                            tgt.updated_date = current_timestamp
# MAGIC               WHEN NOT MATCHED
# MAGIC                 THEN INSERT (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, created_date ) 
# MAGIC                      VALUES (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, current_timestamp)

# COMMAND ----------

drivers_rank = spark.sql("""
SELECT driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points,
       rank() over(order by AVG(calculated_points) desc) as rank
  FROM calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC""")

drivers_rank.write.mode("overwrite").parquet("/mnt/dludemycourse/presentation/drivers_rank")

# COMMAND ----------

team_rank = spark.sql("""
SELECT team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM calculated_race_results
GROUP BY team_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC""")

team_rank.write.mode("overwrite").parquet("/mnt/dludemycourse/presentation/team_rank")
