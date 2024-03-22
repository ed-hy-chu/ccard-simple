-- Databricks notebook source
-- MAGIC %md ## Azure Blob Storage Setup

-- COMMAND ----------

-- MAGIC %md Setting the SAS connection parameters for Azure Blob Storage
-- MAGIC - The SAS token should be granted on the container level
-- MAGIC - The SAS token should provide Read and List permissions

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # TODO: Modify lines 3 and 4 with the correct Azure Storage details
-- MAGIC az_conf_account = "jomoadls1" # Azure Storage account name
-- MAGIC az_conf_container = "sandbox" # Azure Blob Storage container name
-- MAGIC
-- MAGIC # TODO: Modify lines 7 and 8 with the correct Databricks Secret details
-- MAGIC db_conf_scope = "sandbox" # Databricks Secret Scope name
-- MAGIC db_conf_key = "key1" # Databricks Secret Key name
-- MAGIC
-- MAGIC spark.conf.set(f"fs.azure.account.auth.type.{az_conf_account}.dfs.core.windows.net", "SAS")
-- MAGIC spark.conf.set(f"fs.azure.sas.token.provider.type.{az_conf_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
-- MAGIC spark.conf.set(f"fs.azure.sas.fixed.token.{az_conf_account}.dfs.core.windows.net", dbutils.secrets.get(scope=az_conf_container, key=db_conf_key))
-- MAGIC
-- MAGIC # for path shortening
-- MAGIC spark.conf.set("env.azureblob", f"abfss://{az_conf_container}@jomoadls1.dfs.core.windows.net") 

-- COMMAND ----------

-- MAGIC %md Check connection - reading data.csv from Azure Blob Storage

-- COMMAND ----------

SELECT COUNT(1) FROM read_files('${env.azureblob}/card_simple/data.csv')

-- COMMAND ----------

SELECT * FROM read_files('${env.azureblob}/card_simple/data.csv')
LIMIT 5

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.csv(f'{spark.conf.get("env.azureblob")}/card_simple/data.csv')
-- MAGIC df.head(5)

-- COMMAND ----------

-- MAGIC %md Check field length

-- COMMAND ----------

--SELECT char_length(Transaction_id) txn_id_len
--FROM read_files('${env.azureblob}/card_simple/data.csv')
--ORDER BY txn_id_len DESC;

--SELECT char_length(customer_key) ckey_len
--FROM read_files('${env.azureblob}/card_simple/data.csv')
--ORDER BY ckey_len DESC;

SELECT transaction_amount
FROM read_files('${env.azureblob}/card_simple/data.csv')
ORDER BY transaction_amount DESC;

-- COMMAND ----------

-- MAGIC %md ## Credit Card Transaction Data Ingestion

-- COMMAND ----------

-- MAGIC %md Ingest raw data (CSV) to an external table

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ccard_simple;

-- COMMAND ----------

USE SCHEMA ccard_simple;

-- COMMAND ----------

DROP TABLE IF EXISTS raw_txn_data_ext;
CREATE TABLE raw_txn_data_ext (
  txn_id CHAR(36),
  txn_date CHAR(10),
  post_date CHAR(10),
  txn_amt DECIMAL(10,2),
  txn_desc VARCHAR(100),
  cust_key CHAR(16)
) USING CSV
  OPTIONS (header="true", delimiter=",")
  LOCATION '${env.azureblob}/card_simple/data.csv';

SELECT * FROM raw_txn_data_ext;

-- COMMAND ----------

-- MAGIC %md Ingest the data from an external table into a delta table

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze_txn_data (
  txn_id CHAR(36),
  txn_date DATE,
  post_date DATE,
  txn_amt DECIMAL(10,2),
  txn_desc VARCHAR(100),
  cust_key CHAR(16)
);

-- COMMAND ----------

INSERT INTO bronze_txn_data
SELECT
  txn_id,
  to_date(txn_date, 'yyyy-MM-dd'),
  to_date(post_date, 'yyyy-MM-dd'),
  txn_amt,
  trim(TRAILING FROM txn_desc),
  cust_key
FROM raw_txn_data_ext;

-- COMMAND ----------

DESCRIBE DETAIL bronze_txn_data;

-- COMMAND ----------

SELECT * FROM bronze_txn_data;

-- COMMAND ----------

-- MAGIC %md ##Load Regex
-- MAGIC

-- COMMAND ----------

-- MAGIC %md Load regex.csv as aux_regex_ext (External Table) / aux_regex (Delta Table) for **03 Clean Description**

-- COMMAND ----------

USE SCHEMA ccard_simple;
DROP TABLE IF EXISTS aux_regex_ext;
CREATE TABLE aux_regex_ext (
  name STRING,
  regex STRING
) USING CSV
  OPTIONS (header="true", delimiter=",")
  LOCATION '${env.azureblob}/card_simple/regex.csv';

SELECT * FROM aux_regex_ext;

-- COMMAND ----------

CREATE OR REPLACE TABLE aux_regex (
  name STRING,
  regex STRING
);

-- COMMAND ----------

INSERT INTO aux_regex
SELECT name, regex FROM aux_regex_ext;

-- COMMAND ----------

SELECT * FROM aux_regex

-- COMMAND ----------

-- MAGIC %md ## Load Calendar

-- COMMAND ----------

-- MAGIC %md Load calendar.csv as aux_calendar_ext (External Table) / aux_calendar (Delta Table) for **04 Calculate Delay (Method 1)**

-- COMMAND ----------

USE SCHEMA ccard_simple;
DROP TABLE IF EXISTS aux_calendar_ext;
CREATE TABLE aux_calendar_ext (
  day DATE,
  is_nonworking CHAR(1)
) USING CSV
  OPTIONS (header="true", delimiter=",")
  LOCATION '${env.azureblob}/card_simple/calendar.csv';

SELECT * FROM aux_calendar_ext;

-- COMMAND ----------

CREATE OR REPLACE TABLE aux_calendar (
  day DATE,
  is_nonworking CHAR(1)
);

-- COMMAND ----------

INSERT INTO aux_calendar
SELECT * FROM aux_calendar_ext;

-- COMMAND ----------

SELECT * FROM aux_calendar

-- COMMAND ----------

-- MAGIC %md Load calendar.csv as aux_calendarseq_ext (External Table) / aux_calendarseq (Delta Table) for **04 Calculate Delay (Method 2)**

-- COMMAND ----------

USE SCHEMA ccard_simple;
DROP TABLE IF EXISTS aux_calendarseq_ext;
CREATE TABLE aux_calendarseq_ext (
  day DATE,
  is_nonworking CHAR(1),
  seq SMALLINT
) USING CSV
  OPTIONS (header="true", delimiter=",")
  LOCATION '${env.azureblob}/card_simple/calendarseq.csv';

SELECT * FROM aux_calendarseq_ext;

-- COMMAND ----------

CREATE OR REPLACE TABLE aux_calendarseq (
  day DATE,
  seq SMALLINT
);

-- COMMAND ----------

INSERT INTO aux_calendarseq
SELECT day, seq FROM aux_calendarseq_ext;

-- COMMAND ----------

SELECT * FROM aux_calendarseq
