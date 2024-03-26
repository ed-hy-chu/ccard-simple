-- Databricks notebook source
USE ccard_simple;

-- COMMAND ----------

-- MAGIC %md ##Clean description field (Remove UK phone numbers, UK post codes and specified keywords)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Method 1**: regexp_replace by matching every regular expression in aux_regex using a for loop
-- MAGIC
-- MAGIC Suitable for situations with a lower data volume

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import regexp_replace
-- MAGIC
-- MAGIC # Replace all matching patterns in bronze_txn_data.txn_desc using aux_regex
-- MAGIC data = spark.sql("SELECT * FROM bronze_txn_data")
-- MAGIC data_cleaned = data.withColumn("desc_cleaned", data["txn_desc"])
-- MAGIC
-- MAGIC regex_df = spark.sql("SELECT regex FROM aux_regex ORDER BY priority")
-- MAGIC regex_rows = regex_df.collect()
-- MAGIC for row in regex_rows:
-- MAGIC     data_cleaned = data_cleaned.withColumn("desc_cleaned", regexp_replace(data_cleaned["desc_cleaned"], f"{row.regex}", ""))
-- MAGIC
-- MAGIC # Remove extra spaces
-- MAGIC data_cleaned = data_cleaned.withColumn("desc_cleaned", regexp_replace(data_cleaned["desc_cleaned"], "  ", " "))
-- MAGIC
-- MAGIC data_cleaned.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data_cleaned.select('txn_id','txn_date','post_date','txn_amt','desc_cleaned','cust_key').createOrReplaceGlobalTempView("silver_txn_data_tmpvw")

-- COMMAND ----------

SELECT * FROM global_temp.silver_txn_data_tmpvw
