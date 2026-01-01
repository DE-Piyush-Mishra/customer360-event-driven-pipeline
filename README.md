# Customer360 – Event Driven Data Pipeline

This project shows how I built an event-driven data pipeline on Azure.
Whenever a file arrives in the landing folder, the pipeline runs automatically,
validates the data, and moves the file based on business rules.

This project was built to understand how real-time file ingestion works in
production using Azure services.

## What this pipeline does
- Watches a landing folder in ADLS Gen2 for new files
- Triggers an Azure Data Factory pipeline using a storage event
- Calls a Databricks notebook to validate the data
- Checks for:
  - Duplicate order_id values
  - Valid order_status values using a lookup table
- Moves the file to:
  - `staging/` if all checks pass
  - `discarded/` if any validation fails

## Tech used
- Azure Data Factory (Storage Event Trigger)
- Azure Databricks (PySpark)
- Azure Data Lake Storage Gen2
- Azure SQL Database (lookup table)
- Azure Key Vault (secrets)

## Folder structure
landing/  
staging/  
discarded/

## Notes
- File names are handled dynamically (no hardcoding)
- Secrets are managed using Key Vault
- The pipeline is designed to be reusable and easy to extend

## How the pipeline works

1. A file is dropped into the ADLS Gen2 `landing` folder.
2. A storage event trigger in Azure Data Factory starts the pipeline.
3. ADF passes the file name dynamically to a Databricks notebook.
4. The Databricks notebook:
   - Reads the file from the landing folder
   - Checks for duplicate `order_id`
   - Validates `order_status` using a lookup table
   - Moves the file to `staging` or `discarded` based on the result
5. The notebook exits with a status that can be tracked in ADF monitoring.


