-- Databricks notebook source
USE ccard_simple;

-- COMMAND ----------

-- MAGIC %md ##Clean description field (Remove UK phone numbers, UK post codes and specified keywords)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import regexp_replace, lit, col, expr
-- MAGIC
-- MAGIC data_df = spark.sql("SELECT *, txn_desc AS desc_clean FROM bronze_txn_data")
-- MAGIC regex_df = spark.sql("SELECT * FROM aux_regex")
-- MAGIC
-- MAGIC joined_df = data_df.join(regex_df, expr("desc_clean RLIKE aux_regex.regex"), "left")
-- MAGIC glob_clean_df = joined_df.where(expr("aux_regex.regex IS NULL")) # Global cleansed pool: Add rows that do not need cleansing (no regex match at all)
-- MAGIC glob_clean_df = glob_clean_df.select("txn_id", "txn_date", "post_date", "txn_amt", "txn_desc", "cust_key", "desc_clean")
-- MAGIC unclean_df = joined_df.where(expr("aux_regex.regex IS NOT NULL")) # Rows that need further cleansing
-- MAGIC unclean_df = unclean_df.select("txn_id", "txn_date", "post_date", "txn_amt", "txn_desc", "cust_key", "desc_clean")
-- MAGIC
-- MAGIC glob_clean_df.display()
-- MAGIC
-- MAGIC # Recursive function to clean description
-- MAGIC def clean_str(df):
-- MAGIC     global regex_df, glob_clean_df
-- MAGIC
-- MAGIC     # pass 1
-- MAGIC     df = df.join(regex_df, expr("desc_clean RLIKE aux_regex.regex"), "left")
-- MAGIC     df = df.withColumn("desc_clean", regexp_replace(df.desc_clean, df.regex, "")) # Clean
-- MAGIC     df = df.select("txn_id", "txn_date", "post_date", "txn_amt", "txn_desc", "cust_key", "desc_clean")
-- MAGIC
-- MAGIC     joined_df = df.join(regex_df, expr("desc_clean RLIKE aux_regex.regex"), "left") # Join again to see which rows are still unclean
-- MAGIC     clean_df = joined_df.where(expr("aux_regex.regex IS NULL")) # Rows that do not need cleansing (no regex match at all)
-- MAGIC     clean_df = clean_df.select("txn_id", "txn_date", "post_date", "txn_amt", "txn_desc", "cust_key", "desc_clean")
-- MAGIC     unclean_df = joined_df.where(expr("aux_regex.regex IS NOT NULL")) # Rows that need further cleansing
-- MAGIC     unclean_df = unclean_df.select("txn_id", "txn_date", "post_date", "txn_amt", "txn_desc", "cust_key", "desc_clean")
-- MAGIC
-- MAGIC     clean_df = clean_df.intersect(clean_df) # Remove duplicates
-- MAGIC     glob_clean_df = glob_clean_df.union(clean_df) # Add the newly cleansed rows to the global cleansed pool
-- MAGIC     glob_clean_df.display()
-- MAGIC     
-- MAGIC     # pass 2
-- MAGIC     df = unclean_df.join(regex_df, expr("desc_clean RLIKE aux_regex.regex"), "left")
-- MAGIC     df = df.withColumn("desc_clean", regexp_replace(df.desc_clean, df.regex, "")) # Clean
-- MAGIC     df = df.select("txn_id", "txn_date", "post_date", "txn_amt", "txn_desc", "cust_key", "desc_clean")
-- MAGIC
-- MAGIC     joined_df = df.join(regex_df, expr("desc_clean RLIKE aux_regex.regex"), "left") # Join again to see which rows are still unclean
-- MAGIC     clean_df = joined_df.where(expr("aux_regex.regex IS NULL")) # Rows that do not need cleansing (no regex match at all)
-- MAGIC     clean_df = clean_df.select("txn_id", "txn_date", "post_date", "txn_amt", "txn_desc", "cust_key", "desc_clean")
-- MAGIC     unclean_df = joined_df.where(expr("aux_regex.regex IS NOT NULL")) # Rows that need further cleansing
-- MAGIC     unclean_df = unclean_df.select("txn_id", "txn_date", "post_date", "txn_amt", "txn_desc", "cust_key", "desc_clean")
-- MAGIC
-- MAGIC     clean_df = clean_df.intersect(clean_df) # Remove duplicates
-- MAGIC     glob_clean_df = glob_clean_df.union(clean_df) # Add the newly cleansed rows to the global cleansed pool
-- MAGIC     glob_clean_df.display()
-- MAGIC
-- MAGIC     # pass 3
-- MAGIC     df = unclean_df.join(regex_df, expr("desc_clean RLIKE aux_regex.regex"), "left")
-- MAGIC     df = df.withColumn("desc_clean", regexp_replace(df.desc_clean, df.regex, "")) # Clean
-- MAGIC     df = df.select("txn_id", "txn_date", "post_date", "txn_amt", "txn_desc", "cust_key", "desc_clean")
-- MAGIC
-- MAGIC     joined_df = df.join(regex_df, expr("desc_clean RLIKE aux_regex.regex"), "left") # Join again to see which rows are still unclean
-- MAGIC     clean_df = joined_df.where(expr("aux_regex.regex IS NULL")) # Rows that do not need cleansing (no regex match at all)
-- MAGIC     clean_df = clean_df.select("txn_id", "txn_date", "post_date", "txn_amt", "txn_desc", "cust_key", "desc_clean")
-- MAGIC     unclean_df = joined_df.where(expr("aux_regex.regex IS NOT NULL")) # Rows that need further cleansing
-- MAGIC     unclean_df = unclean_df.select("txn_id", "txn_date", "post_date", "txn_amt", "txn_desc", "cust_key", "desc_clean")
-- MAGIC
-- MAGIC     clean_df = clean_df.intersect(clean_df) # Remove duplicates
-- MAGIC     glob_clean_df = glob_clean_df.union(clean_df) # Add the newly cleansed rows to the global cleansed pool
-- MAGIC     glob_clean_df.display()
-- MAGIC
-- MAGIC     return df
-- MAGIC
-- MAGIC # Function call
-- MAGIC clean_str(unclean_df)
-- MAGIC #ho.display()
-- MAGIC
-- MAGIC # Remove extra spaces
-- MAGIC #data_cleaned = data_cleaned.withColumn("desc_cleaned", regexp_replace(data_cleaned["desc_cleaned"], "  ", " "))
-- MAGIC
-- MAGIC #data_cleaned.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import regexp_replace, lit, col, expr
-- MAGIC
-- MAGIC data_df = spark.sql("SELECT *, txn_desc AS desc_clean FROM bronze_txn_data")
-- MAGIC regex_df = spark.sql("SELECT * FROM aux_regex")
-- MAGIC
-- MAGIC joined_df = data_df.join(regex_df, expr("desc_clean RLIKE aux_regex.regex"), "left")
-- MAGIC glob_clean_df = joined_df.where(expr("aux_regex.regex IS NULL")) # Global cleansed pool: Add rows that do not need cleansing (no regex match at all)
-- MAGIC glob_clean_df = glob_clean_df.select("txn_id", "txn_date", "post_date", "txn_amt", "txn_desc", "cust_key", "desc_clean")
-- MAGIC unclean_df = joined_df.where(expr("aux_regex.regex IS NOT NULL")) # Rows that need further cleansing
-- MAGIC unclean_df = unclean_df.select("txn_id", "txn_date", "post_date", "txn_amt", "txn_desc", "cust_key", "desc_clean")
-- MAGIC
-- MAGIC # Recursive function to clean description
-- MAGIC def clean_str(df):
-- MAGIC     global regex_df, glob_clean_df
-- MAGIC
-- MAGIC     if df.isEmpty() == False:
-- MAGIC         df = df.join(regex_df, expr("desc_clean RLIKE aux_regex.regex"), "left")
-- MAGIC         df = df.withColumn("desc_clean", regexp_replace(df.desc_clean, df.regex, "")) # Clean
-- MAGIC         df = df.select("txn_id", "txn_date", "post_date", "txn_amt", "txn_desc", "cust_key", "desc_clean")
-- MAGIC
-- MAGIC         joined_df = df.join(regex_df, expr("desc_clean RLIKE aux_regex.regex"), "left") # Join again to see which rows are still unclean
-- MAGIC         clean_df = joined_df.where(expr("aux_regex.regex IS NULL")) # Rows that do not need further cleansing
-- MAGIC         clean_df = clean_df.select("txn_id", "txn_date", "post_date", "txn_amt", "txn_desc", "cust_key", "desc_clean")
-- MAGIC         unclean_df = joined_df.where(expr("aux_regex.regex IS NOT NULL")) # Rows that need further cleansing
-- MAGIC         unclean_df = unclean_df.select("txn_id", "txn_date", "post_date", "txn_amt", "txn_desc", "cust_key", "desc_clean")
-- MAGIC
-- MAGIC         clean_df = clean_df.intersect(clean_df) # Remove duplicates
-- MAGIC         glob_clean_df = glob_clean_df.union(clean_df) # Add the newly cleansed rows to the global cleansed pool
-- MAGIC
-- MAGIC         clean_str(unclean_df)
-- MAGIC     
-- MAGIC     else:
-- MAGIC         return 0
-- MAGIC
-- MAGIC # Function call
-- MAGIC clean_str(unclean_df)
-- MAGIC glob_clean_df.display()
-- MAGIC
-- MAGIC # Remove extra spaces
-- MAGIC #data_cleaned = data_cleaned.withColumn("desc_cleaned", regexp_replace(data_cleaned["desc_cleaned"], "  ", " "))
-- MAGIC
-- MAGIC #data_cleaned.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data_cleaned.select('txn_id','txn_date','post_date','txn_amt','desc_cleaned','cust_key').createOrReplaceGlobalTempView("silver_txn_data_tmpvw")

-- COMMAND ----------

SELECT * FROM global_temp.silver_txn_data_tmpvw
