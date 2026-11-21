# Databricks notebook source
from pyspark.sql import functions as F

# 1. Read the Bronze Delta table
bronze_df = spark.table(
    "workspace.bronze.dataco_supply_chain_raw"
)

# 2. Remove unnecessary personal and non-analytical columns
pii_columns = [
    "customer_email",
    "customer_fname",
    "customer_lname",
    "customer_password",
    "customer_street",
    "customer_zipcode",
    "customer_city",
    "customer_state",
    "customer_country"
]

silver_df = bronze_df.drop(*pii_columns)

# 3. Convert date columns into timestamps
silver_df = (
    silver_df
    .withColumn(
        "order_date",
        F.to_timestamp(
            F.col("order_date_dateorders"),
            "M/d/yyyy H:mm"
        )
    )
    .withColumn(
        "shipping_date",
        F.to_timestamp(
            F.col("shipping_date_dateorders"),
            "M/d/yyyy H:mm"
        )
    )
    .drop(
        "order_date_dateorders",
        "shipping_date_dateorders"
    )
)

# 4. Convert ZIP code into string
silver_df = silver_df.withColumn(
    "order_zipcode",
    F.col("order_zipcode").cast("string")
)

# 5. Explicitly convert numeric columns
numeric_columns = {
    "days_for_shipping_real": "integer",
    "days_for_shipment_scheduled": "integer",
    "late_delivery_risk": "integer",
    "order_item_quantity": "integer",
    "sales": "double",
    "order_item_total": "double",
    "order_item_profit_ratio": "double",
    "order_profit_per_order": "double",
    "benefit_per_order": "double",
    "order_item_discount": "double",
    "order_item_discount_rate": "double",
    "product_price": "double",
    "sales_per_customer": "double",
    "category_id": "integer",
    "customer_id": "integer",
    "department_id": "integer",
    "latitude": "double",
    "longitude": "double",
    "order_customer_id": "integer",
    "order_id": "integer",
    "order_item_cardprod_id": "integer",
    "order_item_id": "integer",
    "order_item_product_price": "double",
    "product_card_id": "integer",
    "product_category_id": "integer",
    "product_status": "integer"
}

for column_name, data_type in numeric_columns.items():
    if column_name in silver_df.columns:
        silver_df = silver_df.withColumn(
            column_name,
            F.col(column_name).cast(data_type)
        )

# 6. Write Silver table with schema replacement
(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.silver.dataco_supply_chain_clean")
)

print("Silver transformation completed successfully!")

# COMMAND ----------


silver_table = spark.table(
    "workspace.silver.dataco_supply_chain_clean"
)

print("Silver Records:", silver_table.count())
print("Silver Columns:", len(silver_table.columns))

silver_table.printSchema()

display(silver_table.limit(10))


# COMMAND ----------


from pyspark.sql import functions as F

# Read existing Silver table
silver_df = spark.table(
    "workspace.silver.dataco_supply_chain_clean"
)

# Define correct data types
type_mapping = {
    "sales_per_customer": "double",
    "category_id": "integer",
    "customer_id": "integer",
    "department_id": "integer",
    "latitude": "double",
    "longitude": "double",
    "order_customer_id": "integer",
    "order_id": "integer",
    "order_item_cardprod_id": "integer",
    "order_item_id": "integer",
    "order_item_product_price": "double",
    "product_card_id": "integer",
    "product_category_id": "integer",
    "product_status": "integer"
}

# Apply type conversions
for column_name, data_type in type_mapping.items():
    silver_df = silver_df.withColumn(
        column_name,
        F.col(column_name).cast(data_type)
    )

# Save the corrected Silver table
(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.silver.dataco_supply_chain_clean")
)

print("Silver data types corrected successfully!")


# COMMAND ----------

from pyspark.sql import functions as F

silver_df = spark.table("workspace.silver.dataco_supply_chain_clean")

print("Current schema:")
silver_df.printSchema()

print("sales_per_customer datatype:")
print(silver_df.schema["sales_per_customer"].dataType)

display(
    silver_df.select("sales_per_customer").limit(10)
)

# COMMAND ----------


silver_table = spark.table(
    "workspace.silver.dataco_supply_chain_clean"
)

print("Records:", silver_table.count())
print("Columns:", len(silver_table.columns))

silver_table.printSchema()


# COMMAND ----------


from pyspark.sql import functions as F

silver_table = spark.table(
    "workspace.silver.dataco_supply_chain_clean"
)

# Check nulls in converted numeric and date columns
validation_columns = [
    "sales_per_customer",
    "category_id",
    "customer_id",
    "latitude",
    "longitude",
    "order_id",
    "order_item_id",
    "order_item_product_price",
    "order_date",
    "shipping_date"
]

display(
    silver_table.select([
        F.count(
            F.when(F.col(c).isNull(), 1)
        ).alias(c)
        for c in validation_columns
    ])
)


# COMMAND ----------


# Check invalid shipping durations
print(
    "Negative actual shipping days:",
    silver_table.filter(
        F.col("days_for_shipping_real") < 0
    ).count()
)

print(
    "Negative scheduled shipping days:",
    silver_table.filter(
        F.col("days_for_shipment_scheduled") < 0
    ).count()
)

# Check invalid delivery risk values
display(
    silver_table.groupBy("late_delivery_risk").count()
)

# Check date inconsistencies
print(
    "Shipping before order:",
    silver_table.filter(
        F.col("shipping_date") < F.col("order_date")
    ).count()
)


# COMMAND ----------

