-- Databricks notebook source
USE ccard_simple;

-- COMMAND ----------

-- MAGIC %md Finding the range of dates

-- COMMAND ----------

SELECT MAX(txn_date), MIN(txn_date), MAX(post_date), MIN(post_date)
FROM bronze_txn_data
