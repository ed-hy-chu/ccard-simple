# Mini Project on Credit Card Transaction Data

## System Environment
- Developed and tested on Databricks Runtime 14.3 LTS ML

## Setup of Secret Scope and Secret
This project assumes that the raw data file (CSV) is stored on Azure Blob Storage. SAS token authentication is used in this project. To complete the authentication setup, create a SAS token for the container that contains the raw data file. "Read" and "List" permissions are required. Afterwards, go through the following steps.
1. Using Databricks CLI, run:
> `databricks secrets create-scope sandbox`

> Note: Replace `sandbox` with another scope name of your choice when preferred.

2. Prepare a JSON file containing the Azure Blob Storage SAS token (Replace `%sas-token%` with the actual SAS token). Content of the JSON file:
> `{ "scope": "sandbox", "key": "key1", "string_value": "%sas-token%" }`

> Note: Replace `sandbox` with the scope name used in Step 1. In notebook **01 Data Setup**, modify line 7 if the scope name is not `sandbox`.

> Note: Replace `key1` with another key name of your choice when preferred. In notebook **01 Data Setup**, modify line 8 if the scope name is not `key1`.

3. Add the JSON file as a secret to the secret scope by running:
> `databricks secrets put-secret --json @/path/to/file.json`

4. In notebook **01 Data Setup**, modify lines 3 and 4 accordingly with the actual name of the container used on Azure Blob Storage.

## Data Files
The data files are provided in the `Data` directory. The notebook assumes the following files are stored on Azure Blob Storage, accessed through **01 Data Setup**.
- `data.csv` containing the raw data to be transformed
- `calendar.csv` calendar lookup table for **04 Calculate Delay (Method 1)**
- `calendarseq.csv` calendar lookup table for **04 Calculate Delay (Method 2)**
- `regex.csv` list of regular expressions to be matched in **03 Clean Description**

## Job Setup
To run the notebooks as a job, the following task sequences are recommended and have been tested:
> **01 Data Setup** → **03 Clean Description (Method 1)** → **04 Calculate Delay (Method 2)**

or

> **01 Data Setup** → **03 Clean Description (Method 2)** → **04 Calculate Delay (Method 2)**