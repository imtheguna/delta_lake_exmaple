# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS DELTA_DEMO
# MAGIC LOCATION '/mnt/formula1dlg/deltademo'

# COMMAND ----------

file1 = spark.read \
    .option('infreschema',True) \
    .option('header','true') \
    .csv('/mnt/formula1dlg/dqf/raw/Netflix_Userbase_20240203.csv')

# COMMAND ----------

file2 = spark.read \
    .option('infreschema',True) \
    .option('header','true') \
    .csv('/mnt/formula1dlg/dqf/raw/Netflix_Userbase_20240204.csv')

# COMMAND ----------

file1.write.format('delta').mode('overwrite').option("overwriteSchema", "true").saveAsTable('DELTA_DEMO.file1')
file2.write.format('delta').mode('overwrite').option("overwriteSchema", "true").saveAsTable('DELTA_DEMO.file2')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY DELTA_DEMO.file1

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into DELTA_DEMO.file1
# MAGIC select * from DELTA_DEMO.file2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Before One Records Insert' as Status, count(*) as count FROM DELTA_DEMO.file1 VERSION AS OF 0
# MAGIC union 
# MAGIC SELECT 'After One Records Insert' as Status, count(*) as count FROM DELTA_DEMO.file1 VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Before One Records Insert' as Status, count(*) as count FROM DELTA_DEMO.file1 TIMESTAMP AS OF '2024-02-21T13:54:29Z'
# MAGIC union 
# MAGIC SELECT 'After One Records Insert' as Status, count(*) as count FROM DELTA_DEMO.file1 TIMESTAMP AS OF 
# MAGIC '2024-02-21T13:38:02Z'

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", '2024-02-21T13:38:02Z').load("/mnt/formula1dlg/deltademo/file1")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM DELTA_DEMO.file1

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY DELTA_DEMO.file1

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM DELTA_DEMO.file1 RETAIN 0 hours 
