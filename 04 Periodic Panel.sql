-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Periodic Panel to display transaction information
-- MAGIC Obtain
-- MAGIC `Customer Key`, `Last Day of the Current Week`, `Total Spent Amount`, `Total Active Days`, `Total Transaction Count`
-- MAGIC
-- MAGIC for the recent `4`, `9`, `13`, `26` and `52` weeks time windows (referred as `N`)
-- MAGIC
-- MAGIC Details of the requirements:
-- MAGIC - Only show active customers (Active = Has a transaction in the recent time window and also a transaction in the same time window last year)
-- MAGIC
-- MAGIC Parameter:
-- MAGIC - `today`: Date of comparison in YYYY-MM-DD (e.g. If `today` is set as `2020-08-27`, the current time window for 4 weeks is from `2020-08-03` to `2020-08-30`, where `2020-08-30` is the last day of the week of `2020-08-27`. The previous time window is from `2019-08-03` to `2019-08-30`.)

-- COMMAND ----------

USE ccard_simple;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Prepare gold tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC weeks = [4, 9, 13, 26, 52]
-- MAGIC for w in weeks:
-- MAGIC     spark.sql(f"""
-- MAGIC             CREATE OR REPLACE TABLE gold_panel_{w} (
-- MAGIC             cust_key STRING,
-- MAGIC             last_day_of_curr_wk DATE,
-- MAGIC             total_spent_amt DECIMAL(10,2),
-- MAGIC             total_active_days INT,
-- MAGIC             total_txn_cnt INT
-- MAGIC             );
-- MAGIC         """)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gold_panel (
  cust_key STRING,
  N SMALLINT,
  last_day_of_curr_wk DATE,
  total_spent_amt DECIMAL(10,2),
  total_active_days INT,
  total_txn_cnt INT
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Insert aggregated results into gold tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import date_add
-- MAGIC from datetime import datetime, timedelta
-- MAGIC from dateutil.relativedelta import relativedelta
-- MAGIC
-- MAGIC def get_last_day_of_wk(today) -> str:
-- MAGIC     # Look up calendar.csv to obtain the last day of week
-- MAGIC     df = spark.sql(f"""
-- MAGIC                 SELECT date_format(C.day, "yyyy-MM-dd") AS last_day_of_wk
-- MAGIC                 FROM 
-- MAGIC                 (
-- MAGIC                     SELECT day, wk_seq FROM
-- MAGIC                         (SELECT *, row_number() OVER (PARTITION BY wk_seq ORDER BY day DESC) AS day_of_wk_reverse
-- MAGIC                         FROM aux_calendarseq)
-- MAGIC                     WHERE day_of_wk_reverse = 1
-- MAGIC                 ) C
-- MAGIC                 WHERE '{today}' <= C.day
-- MAGIC                 AND date_add('{today}', 7) > C.day
-- MAGIC                 LIMIT 1
-- MAGIC               """)
-- MAGIC     day = df.collect()
-- MAGIC     return day[0].last_day_of_wk
-- MAGIC
-- MAGIC def run_panel(N, last_day_of_wk):
-- MAGIC     """
-- MAGIC     N (int): The number of weeks in a time window / period
-- MAGIC     last_day_of_wk: The last day of the final week in the time window / period
-- MAGIC     """
-- MAGIC     # Date calculation for periods
-- MAGIC     curr_end_day : str = last_day_of_wk
-- MAGIC     curr_end_day_dt : datetime = datetime.strptime(curr_end_day, '%Y-%m-%d')
-- MAGIC     curr_start_day : str = (datetime.strptime(curr_end_day, '%Y-%m-%d') - timedelta(days=7*N-1)).strftime('%Y-%m-%d')
-- MAGIC     curr_start_day_dt : datetime = datetime.strptime(curr_start_day, '%Y-%m-%d')
-- MAGIC
-- MAGIC     print(f"curr period start: {curr_start_day}")
-- MAGIC     print(f"curr period end: {curr_end_day}")
-- MAGIC
-- MAGIC     prev_start_day_dt : datetime = curr_start_day_dt - relativedelta(years=1)
-- MAGIC     prev_end_day_dt : datetime = curr_end_day_dt - relativedelta(years=1)
-- MAGIC
-- MAGIC     prev_start_day : str = prev_start_day_dt.strftime('%Y-%m-%d')
-- MAGIC     prev_end_day : str = prev_end_day_dt.strftime('%Y-%m-%d')
-- MAGIC
-- MAGIC     print(f"prev period start: {prev_start_day}")
-- MAGIC     print(f"prev period end: {prev_end_day}")
-- MAGIC
-- MAGIC     # Prepare result for each N
-- MAGIC     df = spark.sql(f"""
-- MAGIC             SELECT
-- MAGIC                 T.cust_key,
-- MAGIC                 {N} AS N,
-- MAGIC                 '{curr_end_day}' AS last_day_of_curr_wk,
-- MAGIC                 SUM(T.txn_amt) AS total_spent_amt,
-- MAGIC                 COUNT(DISTINCT T.txn_date) AS total_active_days,
-- MAGIC                 COUNT(1) AS total_txn_cnt
-- MAGIC             FROM global_temp.silver_txn_data_tmpvw T
-- MAGIC             WHERE T.cust_key IN ( --cust_key that appear in both periods
-- MAGIC                     SELECT T1.cust_key
-- MAGIC                     FROM global_temp.silver_txn_data_tmpvw T1
-- MAGIC                     WHERE T1.txn_date >= '{curr_start_day}'
-- MAGIC                     AND T1.txn_date <= '{curr_end_day}'
-- MAGIC                     INTERSECT
-- MAGIC                     SELECT T2.cust_key
-- MAGIC                     FROM global_temp.silver_txn_data_tmpvw T2
-- MAGIC                     WHERE T2.txn_date >= '{prev_start_day}'
-- MAGIC                     AND T2.txn_date <= '{prev_end_day}'
-- MAGIC                 )
-- MAGIC             AND T.txn_date >= '{curr_start_day}'
-- MAGIC             AND T.txn_date <= '{curr_end_day}'
-- MAGIC             GROUP BY T.cust_key
-- MAGIC             """)
-- MAGIC     return df
-- MAGIC
-- MAGIC today : str = dbutils.widgets.get("today") # Obtain from notebook parameter
-- MAGIC last_day_of_wk : str = get_last_day_of_wk(today)
-- MAGIC
-- MAGIC df4 = run_panel(4, last_day_of_wk)
-- MAGIC df9 = run_panel(9, last_day_of_wk)
-- MAGIC df13 = run_panel(13, last_day_of_wk)
-- MAGIC df26 = run_panel(26, last_day_of_wk)
-- MAGIC df52 = run_panel(52, last_day_of_wk)
-- MAGIC
-- MAGIC # Prepare data into the enriched layer
-- MAGIC enriched_df = df4.union(df9)
-- MAGIC enriched_df = enriched_df.union(df13)
-- MAGIC enriched_df = enriched_df.union(df26)
-- MAGIC enriched_df = enriched_df.union(df52)
-- MAGIC enriched_df.createOrReplaceTempView("enriched_panel")
-- MAGIC
-- MAGIC # Insert into gold layer
-- MAGIC spark.sql("TRUNCATE TABLE gold_panel")
-- MAGIC spark.sql("INSERT INTO gold_panel SELECT * FROM enriched_panel")

-- COMMAND ----------

SELECT * FROM gold_panel
ORDER BY N, cust_key
