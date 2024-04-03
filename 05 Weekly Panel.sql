-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Periodic Panel to display transaction information
-- MAGIC Obtain
-- MAGIC `Customer Key`, `Last Day of the Current Week`, `Total Spent Amount`, `Total Active Days`, `Total Transaction Count`
-- MAGIC
-- MAGIC for the recent `4`, `9`, `13`, `26` and `52` weeks time windows
-- MAGIC
-- MAGIC Details of the requirements:
-- MAGIC - Only show active customers (Active = Has a transaction in the recent time window and also a transaction in the same time window last year)

-- COMMAND ----------

USE ccard_simple;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Prepare gold tables

-- COMMAND ----------

DROP TABLE gold_panel_4;
DROP TABLE gold_panel_9;
DROP TABLE gold_panel_13;
DROP TABLE gold_panel_26;
DROP TABLE gold_panel_52;

-- COMMAND ----------

-- MAGIC %python
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

-- MAGIC %md
-- MAGIC Insert aggregated results into gold tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import expr, date_add
-- MAGIC from datetime import datetime, timedelta
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
-- MAGIC def run_panel(N):
-- MAGIC     # Date calculation for periods
-- MAGIC     today = dbutils.widgets.get("today")
-- MAGIC     curr_end_day : str = get_last_day_of_wk(today) # Equivalent to the last day of the week of "today"
-- MAGIC     curr_start_day : str = (datetime.strptime(curr_end_day, '%Y-%m-%d') - timedelta(days=7*N-1)).strftime('%Y-%m-%d')
-- MAGIC     print(f"curr period start: {curr_start_day}")
-- MAGIC     print(f"curr period end: {curr_end_day}")
-- MAGIC
-- MAGIC     curr_end_day_dt : datetime = datetime.strptime(curr_end_day, '%Y-%m-%d')
-- MAGIC     prev_end_day_dt : datetime = datetime.strptime(str(curr_end_day_dt.year-1)\
-- MAGIC                                         + '-' + str(curr_end_day_dt.month)\
-- MAGIC                                         + '-' + str(curr_end_day_dt.day)\
-- MAGIC                                     , '%Y-%m-%d')
-- MAGIC     prev_end_day : str = prev_end_day_dt.strftime('%Y-%m-%d')
-- MAGIC     prev_start_day : str = (prev_end_day_dt - timedelta(days=7*N-1)).strftime('%Y-%m-%d')
-- MAGIC     print(f"prev period start: {prev_start_day}")
-- MAGIC     print(f"prev period end: {prev_end_day}")
-- MAGIC
-- MAGIC     # Result
-- MAGIC     result = spark.sql(f"""
-- MAGIC             INSERT INTO gold_panel_{N}
-- MAGIC             SELECT
-- MAGIC                 T.cust_key,
-- MAGIC                 '{curr_end_day}' AS last_day_of_curr_wk,
-- MAGIC                 SUM(T.txn_amt) AS total_spent_amt,
-- MAGIC                 COUNT(DISTINCT T.txn_date) AS total_active_days,
-- MAGIC                 COUNT(1) AS total_txn_cnt
-- MAGIC             FROM global_temp.silver_txn_data_tmpvw T
-- MAGIC             WHERE T.cust_key IN ( --cust_key that appear in both periods
-- MAGIC                     SELECT cust_key
-- MAGIC                     FROM global_temp.silver_txn_data_tmpvw T
-- MAGIC                     WHERE T.txn_date >= '{curr_start_day}'
-- MAGIC                     AND T.txn_date <= '{curr_end_day}'
-- MAGIC                     INTERSECT
-- MAGIC                     SELECT cust_key
-- MAGIC                     FROM global_temp.silver_txn_data_tmpvw T
-- MAGIC                     WHERE T.txn_date >= '{prev_start_day}'
-- MAGIC                     AND T.txn_date <= '{prev_end_day}'
-- MAGIC                 )
-- MAGIC             AND T.txn_date >= '{curr_start_day}'
-- MAGIC             AND T.txn_date <= '{curr_end_day}'
-- MAGIC             GROUP BY T.cust_key
-- MAGIC             """)
-- MAGIC
-- MAGIC run_panel(4)
-- MAGIC run_panel(9)
-- MAGIC run_panel(13)
-- MAGIC run_panel(26)
-- MAGIC run_panel(52)
