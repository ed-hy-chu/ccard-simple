-- Databricks notebook source
-- MAGIC %md ### Calculate the delay between post_date and txn_date (excluding Sat/Sun/bank holidays)

-- COMMAND ----------

USE ccard_simple;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gold_txn_data (
  txn_id STRING,
  txn_date DATE,
  post_date DATE,
  delay INT,
  txn_amt DECIMAL(10,2),
  txn_desc STRING,
  cust_key STRING
);

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW enriched_txn_data AS
SELECT txn_id, txn_date, post_date, delay, txn_amt, desc_cleaned, cust_key FROM
  (SELECT
    T.txn_id,
    T.txn_date,
    T.post_date,
    (SELECT COUNT(1) FROM aux_calendar C
      WHERE C.is_nonworking = 'Y'
        AND C.day >= T.txn_date
        AND C.day <= T.post_date
    ) AS holiday_count,
    CASE
      WHEN T.txn_date = T.post_date THEN
        0
      WHEN T.txn_date IN (SELECT day FROM aux_calendar WHERE is_nonworking = 'Y') THEN
        datediff(T.post_date, T.txn_date) - holiday_count + 1
      ELSE
        datediff(T.post_date, T.txn_date) - holiday_count
    END AS delay,
    T.txn_amt,
    T.desc_cleaned,
    T.cust_key
  FROM global_temp.silver_txn_data_tmpvw T)

-- COMMAND ----------

MERGE INTO gold_txn_data G
USING enriched_txn_data E
ON G.txn_id = E.txn_id
WHEN MATCHED THEN
  UPDATE SET
    G.txn_date = E.txn_date,
    G.post_date = E.txn_date,
    G.delay = E.delay,
    G.txn_amt = E.txn_amt,
    G.txn_desc = E.desc_cleaned,
    G.cust_key = E.cust_key
WHEN NOT MATCHED THEN
  INSERT (G.txn_id, G.txn_date, G.post_date, G.delay, G.txn_amt, G.txn_desc, G.cust_key)
  VALUES (E.txn_id, E.txn_date, E.post_date, E.delay, E.txn_amt, E.desc_cleaned, E.cust_key)
WHEN NOT MATCHED BY SOURCE THEN
  DELETE

-- COMMAND ----------

SELECT * FROM gold_txn_data
ORDER BY txn_date
