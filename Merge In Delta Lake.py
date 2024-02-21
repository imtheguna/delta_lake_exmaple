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

df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").saveAsTable('DELTA_DEMO.user_org')
df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").saveAsTable('DELTA_DEMO.user_update')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC delete from DELTA_DEMO.user_update where Subscription_Type = 'Basic';
# MAGIC Update DELTA_DEMO.user_update set Country = upper(Country);
# MAGIC
# MAGIC ALTER TABLE DELTA_DEMO.user_update ADD COLUMNS (updatedDate DATE)

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct Subscription_Type from DELTA_DEMO.user_update 

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct Country,Subscription_Type from DELTA_DEMO.user_update 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO DELTA_DEMO.user_update tgt
# MAGIC USING DELTA_DEMO.user_org upd
# MAGIC ON tgt.User_ID = upd.User_ID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.Country = upd.Country,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (User_ID,Subscription_Type,Monthly_Revenue,Join_Date,Last_Payment_Date,Country,Age,Gender,Device,Plan_Duration) VALUES (User_ID,Subscription_Type,Monthly_Revenue,Join_Date,Last_Payment_Date,Country,Age,Gender,Device,Plan_Duration)

# COMMAND ----------

user_org = spark.read.format('delta') \
  .load('/mnt/formula1dlg/deltademo/user_org')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dlg/deltademo/user_update")

deltaTable.alias("tgt").merge(
     user_org.alias("org"),
    "tgt.User_ID = org.User_ID") \
  .whenMatchedUpdate(set = { "Country" : "org.Country", "updatedDate": "current_timestamp()" } ) \
  .whenNotMatchedInsert(values =
    {
      "User_ID" : "org.User_ID",
      "Subscription_Type":"org.Subscription_Type",
      "Monthly_Revenue":"org.Monthly_Revenue",
      "Join_Date":"org.Join_Date",
      "Last_Payment_Date":"org.Last_Payment_Date",
      "Country":"org.Country",
      "Age":"org.Age",
      "Gender":"org.Gender",
      "Device":"org.Device",
      "Plan_Duration":"org.Plan_Duration"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct Country,Subscription_Type,updatedDate from DELTA_DEMO.user_update 
