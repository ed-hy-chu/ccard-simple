-- Databricks notebook source
USE ccard_simple;

-- COMMAND ----------

-- MAGIC %md ### Calculate the delay between post_date and txn_date (excluding Sat/Sun/bank holidays)

-- COMMAND ----------

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
ORDER BY txn_date
