# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.bronze;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.silver;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.gold;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.logistics_raw;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW SCHEMAS IN workspace;
# MAGIC
# MAGIC SHOW VOLUMES IN workspace.default;
# MAGIC

# COMMAND ----------


# Check files inside our raw data volume

display(dbutils.fs.ls("/Volumes/workspace/default/logistics_raw/"))


# COMMAND ----------


# Define the raw dataset path

file_path = "/Volumes/workspace/default/logistics_raw/DataCoSupplyChainDataset.csv"

# Inspect the first few characters of the file
sample = dbutils.fs.head(file_path, 2000)

print(sample)


# COMMAND ----------


# 1. Define the dataset location
file_path = "/Volumes/workspace/default/logistics_raw/DataCoSupplyChainDataset.csv"

# 2. Read CSV using Apache Spark
df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "PERMISSIVE")
    .option("encoding", "UTF-8")
    .load(file_path)
)

# 3. Display first 10 records
display(df.limit(10))


# COMMAND ----------


# Check total number of records
print("Total Records:", df.count())

# Check total number of columns
print("Total Columns:", len(df.columns))

# Display column names
print("Column Names:")
print(df.columns)

# Check data types
df.printSchema()


# COMMAND ----------


from pyspark.sql.functions import current_timestamp, col
import re

# 1. Read the original CSV
raw_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .option("mode", "PERMISSIVE")
    .option("encoding", "UTF-8")
    .load(file_path)
)

# 2. Normalize column names
def clean_column_name(name):
    name = name.lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")

normalized_columns = [
    clean_column_name(column)
    for column in raw_df.columns
]

raw_df = raw_df.toDF(*normalized_columns)

# 3. Add ingestion metadata
bronze_df = (
    raw_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
)

# 4. Write into Delta Lake
(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.bronze.dataco_supply_chain_raw")
)

print("Bronze Delta table created successfully!")


# COMMAND ----------


bronze_table = spark.table(
    "workspace.bronze.dataco_supply_chain_raw"
)

print("Total Records:", bronze_table.count())
print("Total Columns:", len(bronze_table.columns))

display(bronze_table.limit(5))


# COMMAND ----------


from pyspark.sql import functions as F

# Read Bronze table
bronze_table = spark.table(
    "workspace.bronze.dataco_supply_chain_raw"
)

# 1. Check missing and blank values
null_checks = []

for column_name in bronze_table.columns:
    null_checks.append(
        F.sum(
            F.when(
                F.col(f"`{column_name}`").isNull() |
                (F.trim(F.col(f"`{column_name}`")) == ""),
                1
            ).otherwise(0)
        ).alias(column_name)
    )

null_report = bronze_table.agg(*null_checks)

display(null_report)


# COMMAND ----------


# 2. Count duplicate Order Item IDs

total_records = bronze_table.count()

distinct_items = bronze_table.select(
    "order_item_id"
).distinct().count()

print("Total Records:", total_records)
print("Distinct Order Item IDs:", distinct_items)
print("Repeated IDs:", total_records - distinct_items)

# 3. Check duplicate complete records
duplicate_count = (
    total_records - bronze_table.dropDuplicates().count()
)

print("Duplicate Complete Records:", duplicate_count)


# COMMAND ----------


# 4. Analyze delivery status distribution

display(
    bronze_table
    .groupBy("delivery_status")
    .count()
    .orderBy(F.desc("count"))
)


# COMMAND ----------


from pyspark.sql import functions as F

important_columns = [
    "order_item_id",
    "order_id",
    "shipping_mode",
    "delivery_status",
    "late_delivery_risk",
    "days_for_shipping_real",
    "days_for_shipment_scheduled",
    "order_region",
    "order_country",
    "shipping_date_dateorders",
    "order_date_dateorders"
]

null_report = bronze_table.select([
    F.count(
        F.when(
            F.col(c).isNull() | (F.trim(F.col(c)) == ""),
            1
        )
    ).alias(c)
    for c in important_columns
])

display(null_report)


# COMMAND ----------

