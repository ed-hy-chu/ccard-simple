-- Databricks notebook source
USE ccard_simple;

-- COMMAND ----------

-- MAGIC %md ### Calculate the delay between post_date and txn_date (excluding Sat/Sun/bank holidays)

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_txn_data (
  txn_id CHAR(36),
  txn_date DATE,
  post_date DATE,
  delay INT,
  txn_amt DECIMAL(10,2),
  txn_desc VARCHAR(100),
  cust_key CHAR(16)
);

-- COMMAND ----------

INSERT INTO gold_txn_data
SELECT
  T.txn_id,
  T.txn_date,
  T.post_date,
  (
    (SELECT PD.seq FROM aux_calendarseq PD WHERE PD.day = T.post_date LIMIT 1) -
    (SELECT TD.seq FROM aux_calendarseq TD WHERE TD.day = T.txn_date LIMIT 1)
  ) AS delay,
  T.txn_amt,
  T.desc_cleaned,
  T.cust_key
FROM global_temp.silver_txn_data_tmpvw T

-- COMMAND ----------

SELECT * FROM gold_txn_data
ORDER BY txn_date
