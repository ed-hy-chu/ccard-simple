-- Databricks notebook source
USE ccard_simple;

-- COMMAND ----------

-- MAGIC %md ##Clean description field (Remove UK phone numbers, UK post codes and specified keywords)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Rough work space for preparing Method 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC from pyspark.sql.functions import regexp_replace, lit, col, expr, when
-- MAGIC
-- MAGIC data_df = spark.sql("SELECT *, 0 AS regex_chkpt FROM bronze_txn_data")
-- MAGIC data_df.createOrReplaceTempView("data_df")
-- MAGIC regex_df = spark.sql("SELECT priority, regex FROM aux_regex ORDER BY priority")
-- MAGIC regex_df.createOrReplaceTempView("regex_df")
-- MAGIC
-- MAGIC # pass 1
-- MAGIC df = spark.sql("""
-- MAGIC               SELECT * FROM
-- MAGIC               (SELECT
-- MAGIC                 data_df.txn_id,
-- MAGIC                 data_df.txn_desc,
-- MAGIC                 data_df.regex_chkpt,
-- MAGIC                 regex_df.priority,
-- MAGIC                 regex_df.regex,
-- MAGIC               -- row_num for obtaining the first matched row of the same txn_id
-- MAGIC               row_number() OVER (PARTITION BY data_df.txn_id ORDER BY regex_df.priority) AS row_num
-- MAGIC               FROM data_df LEFT JOIN regex_df
-- MAGIC               ON data_df.txn_desc RLIKE regex_df.regex
-- MAGIC               AND data_df.regex_chkpt < regex_df.priority)
-- MAGIC               WHERE row_num = 1
-- MAGIC               ORDER BY txn_id
-- MAGIC               """)
-- MAGIC df.display()
-- MAGIC
-- MAGIC df = df.withColumn("txn_desc", when(df.regex.isNotNull() == True, regexp_replace(df.txn_desc, df.regex, ""))
-- MAGIC                                 .otherwise(df.txn_desc))
-- MAGIC df = df.withColumn("regex_chkpt", lit(df.priority))
-- MAGIC df.display()
-- MAGIC df.createOrReplaceTempView("data_df")
-- MAGIC
-- MAGIC # pass 2
-- MAGIC df = spark.sql("""
-- MAGIC               SELECT * FROM
-- MAGIC               (SELECT
-- MAGIC                 data_df.txn_id,
-- MAGIC                 data_df.txn_desc,
-- MAGIC                 data_df.regex_chkpt,
-- MAGIC                 regex_df.priority,
-- MAGIC                 regex_df.regex,
-- MAGIC               -- row_num for obtaining the first matched row of the same txn_id
-- MAGIC               row_number() OVER (PARTITION BY data_df.txn_id ORDER BY regex_df.priority) AS row_num
-- MAGIC               FROM data_df LEFT JOIN regex_df
-- MAGIC               ON data_df.txn_desc RLIKE regex_df.regex
-- MAGIC               AND data_df.regex_chkpt < regex_df.priority)
-- MAGIC               WHERE row_num = 1
-- MAGIC               ORDER BY txn_id
-- MAGIC               """)
-- MAGIC df.display()
-- MAGIC
-- MAGIC df = df.withColumn("txn_desc", when(df.regex.isNotNull() == True, regexp_replace(df.txn_desc, df.regex, ""))
-- MAGIC                                 .otherwise(df.txn_desc))
-- MAGIC df = df.withColumn("regex_chkpt", lit(df.priority))
-- MAGIC df.display()
-- MAGIC df.createOrReplaceTempView("data_df")
-- MAGIC
-- MAGIC # pass 3
-- MAGIC df = spark.sql("""
-- MAGIC               SELECT * FROM
-- MAGIC               (SELECT
-- MAGIC                 data_df.txn_id,
-- MAGIC                 data_df.txn_desc,
-- MAGIC                 data_df.regex_chkpt,
-- MAGIC                 regex_df.priority,
-- MAGIC                 regex_df.regex,
-- MAGIC               -- row_num for obtaining the first matched row of the same txn_id
-- MAGIC               row_number() OVER (PARTITION BY data_df.txn_id ORDER BY regex_df.priority) AS row_num
-- MAGIC               FROM data_df LEFT JOIN regex_df
-- MAGIC               ON data_df.txn_desc RLIKE regex_df.regex
-- MAGIC               AND data_df.regex_chkpt < regex_df.priority)
-- MAGIC               WHERE row_num = 1
-- MAGIC               ORDER BY txn_id
-- MAGIC               """)
-- MAGIC df.display()
-- MAGIC
-- MAGIC # Remove extra spaces
-- MAGIC #data_cleaned = data_cleaned.withColumn("desc_cleaned", regexp_replace(data_cleaned["desc_cleaned"], "  ", " "))
-- MAGIC
-- MAGIC #data_cleaned.display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Method 2**: Incremental regexp_replace by matching every regular expression in aux_regex using recursion
-- MAGIC
-- MAGIC Suitable for situations with a higher data volume

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import regexp_replace, lit, when
-- MAGIC
-- MAGIC data_df = spark.sql("SELECT *, 0 AS regex_chkpt FROM bronze_txn_data")
-- MAGIC data_df.createOrReplaceTempView("data_df")
-- MAGIC regex_df = spark.sql("SELECT priority, regex FROM aux_regex ORDER BY priority")
-- MAGIC regex_df.createOrReplaceTempView("regex_df")
-- MAGIC
-- MAGIC def clean(df):
-- MAGIC     df.createOrReplaceTempView("data_df")
-- MAGIC     df = spark.sql("""
-- MAGIC                 SELECT * FROM
-- MAGIC                 (SELECT
-- MAGIC                     data_df.txn_id,
-- MAGIC                     data_df.txn_desc,
-- MAGIC                     data_df.regex_chkpt,
-- MAGIC                     regex_df.priority,
-- MAGIC                     regex_df.regex,
-- MAGIC                 -- row_num for obtaining the first matched row of the same txn_id
-- MAGIC                 row_number() OVER (PARTITION BY data_df.txn_id ORDER BY regex_df.priority) AS row_num
-- MAGIC                 FROM data_df LEFT JOIN regex_df
-- MAGIC                 ON data_df.txn_desc RLIKE regex_df.regex
-- MAGIC                 AND data_df.regex_chkpt < regex_df.priority)
-- MAGIC                 WHERE row_num = 1
-- MAGIC                 """)
-- MAGIC
-- MAGIC     if df.where("regex IS NOT NULL").isEmpty():
-- MAGIC         return df
-- MAGIC     else:
-- MAGIC         df = df.withColumn("txn_desc", when(df.regex.isNotNull() == True, regexp_replace(df.txn_desc, df.regex, ""))
-- MAGIC                                         .otherwise(df.txn_desc))
-- MAGIC         df = df.withColumn("regex_chkpt", lit(df.priority))
-- MAGIC         return clean(df)
-- MAGIC
-- MAGIC clean_df = clean(data_df).select("txn_id", "txn_desc")
-- MAGIC
-- MAGIC # Remove extra spaces
-- MAGIC clean_df = clean_df.withColumn("txn_desc", regexp_replace(clean_df["txn_desc"], "  ", " "))
-- MAGIC clean_df.createOrReplaceTempView("clean_df")
-- MAGIC clean_df.display()
-- MAGIC
-- MAGIC # Merge with other columns
-- MAGIC data_cleaned = spark.sql("""
-- MAGIC                          SELECT B.txn_id, B.txn_date, B.post_date, B.txn_amt, C.txn_desc AS desc_cleaned, B.cust_key
-- MAGIC                          FROM bronze_txn_data B LEFT JOIN clean_df C
-- MAGIC                          ON B.txn_id = C.txn_id
-- MAGIC                          """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data_cleaned.select('txn_id','txn_date','post_date','txn_amt','desc_cleaned','cust_key').createOrReplaceGlobalTempView("silver_txn_data_tmpvw")

-- COMMAND ----------

SELECT * FROM global_temp.silver_txn_data_tmpvw
