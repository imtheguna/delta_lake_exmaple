# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS DELTA_DEMO
# MAGIC LOCATION '/mnt/formula1dlg/deltademo'

# COMMAND ----------

df = spark.read \
    .option('infreschema',True) \
    .option('header','true') \
    .csv('/mnt/formula1dlg/dqf/raw/Netflix_Userbase_20240203.csv')

# COMMAND ----------

df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").saveAsTable('DELTA_DEMO.user')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT  Subscription_Type FROM DELTA_DEMO.USER;

# COMMAND ----------

df.write.format('delta').mode('overwrite') \
  .save('/mnt/formula1dlg/deltademo/user_extl')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table delta_demo.user_external
# MAGIC using delta
# MAGIC location '/mnt/formula1dlg/deltademo/user_extl'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM DELTA_DEMO.USER

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC desc DELTA_DEMO.user

# COMMAND ----------

user_external_df = spark.read.format('delta') \
  .load('/mnt/formula1dlg/deltademo/user_extl')

display(user_external_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE DELTA_DEMO.USER
# MAGIC   SET GENDER = CASE WHEN GENDER = 'Male' then 'M'
# MAGIC                 ELSE 'F' END;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dlg/deltademo/user_extl")

deltaTable.update("Subscription_Type = 'Basic' ", { "Monthly_Revenue": "Monthly_Revenue - 1" } )

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM DELTA_DEMO.USER WHERE Subscription_Type = 'Premium'

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dlg/deltademo/user_extl")

deltaTable.delete("Subscription_Type = 'Basic'")
