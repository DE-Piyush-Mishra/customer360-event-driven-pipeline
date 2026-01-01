"""
Databricks notebook used by Azure Data Factory.

This notebook receives the file name dynamically from ADF,
performs validation checks, and moves the file to staging
or discarded folders based on the results.
"""

# ---------------------------------------
# Get filename from ADF
# ---------------------------------------
filename = dbutils.widgets.get("filename")
print(f"Processing file: {filename}")

# ---------------------------------------
# Mount check (generic logic)
# ---------------------------------------
mount_point = "/mnt/sales"

if mount_point not in [m.mountPoint for m in dbutils.fs.mounts()]:
    print("Mounting storage (handled securely using Key Vault)")
    # Actual mount code is intentionally omitted
else:
    print("Storage already mounted")

---------------------------------------
# Define folder paths
 ---------------------------------------

landing_path = f"{mount_point}/landing"
staging_path = f"{mount_point}/staging"
discarded_path = f"{mount_point}/discarded"

# ---------------------------------------
# Read incoming file
# ---------------------------------------
orders_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{landing_path}/{filename}")
)

# ---------------------------------------
# Validation 1: Duplicate order_id
# ---------------------------------------
total_count = orders_df.count()
distinct_count = orders_df.select("order_id").distinct().count()

if total_count != distinct_count:
    print("Duplicate order_id found. Moving file to discarded.")
    dbutils.fs.mv(
        f"{landing_path}/{filename}",
        f"{discarded_path}/{filename}"
    )
    dbutils.notebook.exit("Duplicate order_id detected")

print("Duplicate order_id validation passed")

# ---------------------------------------
# Validation 2: Valid order_status
# ---------------------------------------
# Valid order_status values are maintained in Azure SQL DB.
# SQL connection details are managed securely via Key Vault.

validStatusDf = spark.read.jdbc(url = connectionUrl, table = 'dbo.valid_order_status', properties = connectionProperties )

validStatusDf.createOrReplaceTempView("valid_status")

InvalidRowsDf = spark.sql("select * from orders where order_status not in (select * from valid_status)")


if InvalidRowsDf.count() > 0:
      errorflg = True

if errorflg:
    dbutils.fs.mv( f"{landing_path}/{filename}", f"{discarded_path}/{filename}")
    dbutils.notebook.exit('{"errorflg": "true" , "errormsg" : "order_status is invalid}')
else :
    dbutils.fs.mv(f"{landing_path}/{filename}" , f"{mount_point}/staging")
    dbutils.notebook.exit('{"errorflg": "False" , "errormsg" : "order_status is valid}')

print("Order status validation passed")
dbutils.notebook.exit("File processed successfully")







