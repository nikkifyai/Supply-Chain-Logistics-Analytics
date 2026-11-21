# Databricks notebook source

from pyspark.sql import functions as F

# COMMAND ----------

silver_df = spark.table(
    "workspace.silver.dataco_supply_chain_clean"
)

display(silver_df.limit(10))

# COMMAND ----------

print("Total rows:", silver_df.count())

print("Total columns:", len(silver_df.columns))

silver_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Overall KPIs — consistent definitions (late% over non-cancelled, cancelled excluded from averages)
# Calculate overall logistics KPIs (one row = whole dataset)
# Business definitions (consistent across ALL Gold tables):
#   - a row in Silver = ONE ORDER ITEM (not one shipment)
#   - late_delivery_percentage = late order items / NON-CANCELLED order items * 100
#   - shipping-day averages EXCLUDE 'Shipping canceled' order items

total_order_items = silver_df.count()

non_cancelled = silver_df.filter(
    F.col("delivery_status") != "Shipping canceled"
)

total_non_cancelled = non_cancelled.count()

late_order_items = non_cancelled.filter(
    F.col("delivery_status") == "Late delivery"
).count()


# Create the overall logistics KPI DataFrame

logistics_kpis = silver_df.agg(
    F.count("*").alias("total_order_items"),
    F.countDistinct("order_id").alias("unique_orders"),
    F.sum("sales").alias("total_sales"),
    F.sum("order_profit_per_order").alias("total_profit"),
    F.avg(
        F.when(
            F.col("delivery_status") != "Shipping canceled",
            F.col("days_for_shipping_real")
        )
    ).alias("avg_actual_shipping_days"),
    F.avg(
        F.when(
            F.col("delivery_status") != "Shipping canceled",
            F.col("days_for_shipment_scheduled")
        )
    ).alias("avg_scheduled_shipping_days"),
).withColumn(
    "total_cancelled",
    F.lit(total_order_items - total_non_cancelled)
).withColumn(
    "total_late_shipments",
    F.lit(late_order_items)
).withColumn(
    "late_delivery_percentage",
    F.lit(
        round(late_order_items / total_non_cancelled * 100, 2)
        if total_non_cancelled > 0
        else 0
    )
)

display(logistics_kpis)

# COMMAND ----------

# Save Logistics KPIs as a Gold Delta table

(
    logistics_kpis.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.gold_logistics_kpis")
)

print("Gold Logistics KPIs table created successfully!")

# COMMAND ----------

# Read the saved Gold KPI table

gold_kpis_df = spark.table(
    "workspace.gold.gold_logistics_kpis"
)

display(gold_kpis_df)

# COMMAND ----------

# Analyze shipment performance by delivery status

delivery_performance = (
    silver_df
    .groupBy("delivery_status")
    .agg(
        F.count("*").alias("total_shipments"),
        F.avg("days_for_shipping_real").alias("avg_actual_shipping_days"),
        F.avg("days_for_shipment_scheduled").alias("avg_scheduled_shipping_days"),
        F.avg("late_delivery_risk").alias("avg_late_delivery_risk"),
        F.sum("sales").alias("total_sales"),
        F.sum("order_profit_per_order").alias("total_profit")
    )
    .orderBy("delivery_status")
)

display(delivery_performance)

# COMMAND ----------

# Save Delivery Performance Analysis as a Gold Delta table

(
    delivery_performance.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.gold_delivery_performance")
)

print("Gold Delivery Performance table created successfully!")

# COMMAND ----------

# Read and verify the saved Gold table

gold_delivery_df = spark.table(
    "workspace.gold.gold_delivery_performance"
)

display(gold_delivery_df)

# COMMAND ----------

# DBTITLE 1,Regional analysis — non-cancelled late% denominator, cancelled excluded from avg days
# Regional logistics performance analysis
# late_delivery_percentage = late order items / NON-CANCELLED order items * 100
# avg_delivery_days excludes 'Shipping canceled' order items

regional_analysis = (
    silver_df
    .groupBy("market", "order_region", "order_country")
    .agg(
        F.count("*").alias("total_shipments"),

        F.sum(
            F.when(
                F.col("delivery_status") == "Late delivery", 1
            ).otherwise(0)
        ).alias("late_shipments"),

        F.sum(
            F.when(
                F.col("delivery_status") == "Shipping canceled", 1
            ).otherwise(0)
        ).alias("cancelled_shipments"),

        F.avg(
            F.when(
                F.col("delivery_status") != "Shipping canceled",
                F.col("days_for_shipping_real")
            )
        ).alias("avg_delivery_days"),

        F.sum("sales").alias("total_sales"),

        F.sum("order_profit_per_order").alias("total_profit")
    )
    .withColumn(
        "non_cancelled_shipments",
        F.col("total_shipments") - F.col("cancelled_shipments")
    )
    .withColumn(
        "late_delivery_percentage",
        F.round(
            F.col("late_shipments")
            / F.col("non_cancelled_shipments")
            * 100,
            2
        )
    )
    .orderBy(F.desc("late_delivery_percentage"))
)

display(regional_analysis)

# COMMAND ----------

# Save Regional Analysis to the Gold layer

(
    regional_analysis.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.gold_regional_analysis")
)

print("Gold Regional Analysis table created successfully!")

# COMMAND ----------

# Verify the saved Gold table

gold_regional_df = spark.table(
    "workspace.gold.gold_regional_analysis"
)

display(gold_regional_df)

# COMMAND ----------

# DBTITLE 1,Shipping mode analysis — non-cancelled late% denominator, cancelled excluded from averages
# Analyze logistics performance by shipping mode
# late_delivery_percentage = late order items / NON-CANCELLED order items * 100
# shipping-day averages exclude 'Shipping canceled' order items

shipping_analysis = (
    silver_df
    .groupBy("shipping_mode")
    .agg(
        F.count("*").alias("total_shipments"),

        F.sum(
            F.when(
                F.col("delivery_status") == "Late delivery", 1
            ).otherwise(0)
        ).alias("late_shipments"),

        F.sum(
            F.when(
                F.col("delivery_status") == "Shipping canceled", 1
            ).otherwise(0)
        ).alias("cancelled_shipments"),

        F.avg(
            F.when(
                F.col("delivery_status") != "Shipping canceled",
                F.col("days_for_shipping_real")
            )
        ).alias("avg_actual_shipping_days"),

        F.avg(
            F.when(
                F.col("delivery_status") != "Shipping canceled",
                F.col("days_for_shipment_scheduled")
            )
        ).alias("avg_scheduled_shipping_days"),

        F.sum("sales").alias("total_sales"),

        F.sum(
            "order_profit_per_order"
        ).alias("total_profit")
    )
    .withColumn(
        "non_cancelled_shipments",
        F.col("total_shipments") - F.col("cancelled_shipments")
    )
    .withColumn(
        "late_delivery_percentage",
        F.round(
            F.col("late_shipments")
            / F.col("non_cancelled_shipments")
            * 100,
            2
        )
    )
    .orderBy(F.desc("late_delivery_percentage"))
)

display(shipping_analysis)

# COMMAND ----------

# Save Shipping Mode Analysis to Gold

(
    shipping_analysis.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.gold_shipping_analysis")
)

print("Gold Shipping Analysis table created successfully!")

# COMMAND ----------

# Verify the saved Gold table

gold_shipping_df = spark.table(
    "workspace.gold.gold_shipping_analysis"
)

display(gold_shipping_df)

# COMMAND ----------

# DBTITLE 1,Shipping mode analysis (verification copy) — same consistent definitions
# Re-check shipping mode analysis (same definitions as the saved table):
# late% over non-cancelled order items; averages exclude cancelled

shipping_analysis = (
    silver_df
    .groupBy("shipping_mode")
    .agg(
        F.count("*").alias("total_shipments"),

        F.sum(
            F.when(
                F.col("delivery_status") == "Late delivery", 1
            ).otherwise(0)
        ).alias("late_shipments"),

        F.sum(
            F.when(
                F.col("delivery_status") == "Shipping canceled", 1
            ).otherwise(0)
        ).alias("cancelled_shipments"),

        F.avg(
            F.when(
                F.col("delivery_status") != "Shipping canceled",
                F.col("days_for_shipping_real")
            )
        ).alias("avg_actual_shipping_days"),

        F.avg(
            F.when(
                F.col("delivery_status") != "Shipping canceled",
                F.col("days_for_shipment_scheduled")
            )
        ).alias("avg_scheduled_shipping_days"),

        F.sum("sales").alias("total_sales"),

        F.sum(
            "order_profit_per_order"
        ).alias("total_profit")
    )
    .withColumn(
        "non_cancelled_shipments",
        F.col("total_shipments") - F.col("cancelled_shipments")
    )
    .withColumn(
        "late_delivery_percentage",
        F.round(
            F.col("late_shipments")
            / F.col("non_cancelled_shipments")
            * 100,
            2
        )
    )
    .orderBy(F.desc("late_delivery_percentage"))
)

display(shipping_analysis)

# COMMAND ----------

# Verify all Gold tables

gold_tables = [
    "workspace.gold.gold_logistics_kpis",
    "workspace.gold.gold_delivery_performance",
    "workspace.gold.gold_regional_analysis",
    "workspace.gold.gold_shipping_analysis"
]

for table_name in gold_tables:
    print(f"\nVerifying: {table_name}")
    
    df = spark.table(table_name)
    
    print("Rows:", df.count())
    print("Columns:", len(df.columns))
    
    display(df)

# COMMAND ----------

# DBTITLE 1,Validation — Priority 1 correctness assertions (late% formula, cancelled exclusion, reconciliation)
# VALIDATION — Priority 1 data-correctness checks (read-only, idempotent; safe to re-run)
# Recomputes expectations independently from Silver (source of truth) and asserts:
#   1. late_delivery_percentage == late order items / NON-CANCELLED order items * 100
#      on every Gold table that carries the metric
#   2. shipping-day averages EXCLUDE 'Shipping canceled' order items
#   3. Gold counts reconcile with Silver counts (order items vs unique orders)
#   4. no legacy 'total_delivered' column remains; structure preserved (additive columns)

from pyspark.sql import functions as F

silver_df = spark.table("workspace.silver.dataco_supply_chain_clean")

GOLD_TABLES = [
    "workspace.gold.gold_logistics_kpis",
    "workspace.gold.gold_delivery_performance",
    "workspace.gold.gold_regional_analysis",
    "workspace.gold.gold_shipping_analysis",
]

# Independently recompute expected values from Silver
expected = silver_df.agg(
    F.count("*").alias("items"),
    F.countDistinct("order_id").alias("orders"),
    F.sum(F.when(F.col("delivery_status") == "Shipping canceled", 1).otherwise(0)).alias("cancelled"),
    F.sum(F.when(F.col("delivery_status") == "Late delivery", 1).otherwise(0)).alias("late"),
    F.avg(F.when(F.col("delivery_status") != "Shipping canceled", F.col("days_for_shipping_real"))).alias("avg_actual_days"),
    F.avg(F.when(F.col("delivery_status") != "Shipping canceled", F.col("days_for_shipment_scheduled"))).alias("avg_sched_days"),
).collect()[0]

expected_non_cancelled = expected["items"] - expected["cancelled"]

checks = []

def check(name, condition, detail=""):
    checks.append((name, bool(condition), detail))

# --- 1) Schema checks: renames applied, structure preserved (additive columns only)
gold_cols = {t: spark.table(t).columns for t in GOLD_TABLES}
for t, cols in gold_cols.items():
    check(
        f"no legacy 'total_delivered' column in {t.split('.')[-1]}",
        "total_delivered" not in cols,
    )
check(
    "gold_shipping_analysis has non_cancelled_shipments (additive column)",
    "non_cancelled_shipments" in gold_cols["workspace.gold.gold_shipping_analysis"],
)
check(
    "gold_regional_analysis has non_cancelled_shipments (additive column)",
    "non_cancelled_shipments" in gold_cols["workspace.gold.gold_regional_analysis"],
)

# --- 2) Overall KPI table: consistent formula + cancelled excluded from averages
kpis = spark.table("workspace.gold.gold_logistics_kpis").collect()[0]
check("KPI total_order_items == Silver order items", kpis["total_order_items"] == expected["items"])
check("KPI unique_orders == Silver distinct order_id", kpis["unique_orders"] == expected["orders"])
check("KPI total_cancelled == Silver cancelled", kpis["total_cancelled"] == expected["cancelled"])
check("KPI total_late_shipments == Silver late", kpis["total_late_shipments"] == expected["late"])
check(
    "KPI late% == late / non-cancelled * 100",
    abs(kpis["late_delivery_percentage"] - round(expected["late"] / expected_non_cancelled * 100, 2)) < 1e-9,
)
check(
    "KPI avg actual shipping days exclude cancelled",
    abs(kpis["avg_actual_shipping_days"] - expected["avg_actual_days"]) < 1e-6,
)
check(
    "KPI avg scheduled shipping days exclude cancelled",
    abs(kpis["avg_scheduled_shipping_days"] - expected["avg_sched_days"]) < 1e-6,
)

# --- 3) Row-level formula checks on regional & shipping tables
for t in ["workspace.gold.gold_regional_analysis", "workspace.gold.gold_shipping_analysis"]:
    df = spark.table(t)
    bad_rows = df.filter(
        (F.col("non_cancelled_shipments") != F.col("total_shipments") - F.col("cancelled_shipments"))
        | (F.col("non_cancelled_shipments") <= 0)
        | (F.col("late_shipments") > F.col("non_cancelled_shipments"))
        | F.col("late_delivery_percentage").isNull()
        | (
            F.abs(
                F.col("late_delivery_percentage")
                - F.round(F.col("late_shipments") / F.col("non_cancelled_shipments") * 100, 2)
            )
            > 1e-9
        )
    ).count()
    check(
        f"late% formula consistent on every row of {t.split('.')[-1]}",
        bad_rows == 0,
        f"{bad_rows} bad rows",
    )

# --- 4) Shipping-day averages exclude cancelled, per shipping mode
exp_mode = silver_df.groupBy("shipping_mode").agg(
    F.avg(F.when(F.col("delivery_status") != "Shipping canceled", F.col("days_for_shipping_real"))).alias("exp_actual"),
    F.avg(F.when(F.col("delivery_status") != "Shipping canceled", F.col("days_for_shipment_scheduled"))).alias("exp_sched"),
)
bad_avgs = (
    spark.table("workspace.gold.gold_shipping_analysis")
    .join(exp_mode, "shipping_mode")
    .filter(
        (F.abs(F.col("avg_actual_shipping_days") - F.col("exp_actual")) > 1e-6)
        | (F.abs(F.col("avg_scheduled_shipping_days") - F.col("exp_sched")) > 1e-6)
    )
    .count()
)
check("shipping-mode averages exclude cancelled order items", bad_avgs == 0, f"{bad_avgs} mismatched modes")

# --- 5) Cross-table reconciliation
sa_totals = (
    spark.table("workspace.gold.gold_shipping_analysis")
    .agg(
        F.sum("total_shipments").alias("items"),
        F.sum("cancelled_shipments").alias("cancelled"),
        F.sum("late_shipments").alias("late"),
    )
    .collect()[0]
)
check("shipping modes sum to Silver order items", sa_totals["items"] == expected["items"])
check("shipping modes sum to Silver cancelled", sa_totals["cancelled"] == expected["cancelled"])
check("shipping modes sum to Silver late", sa_totals["late"] == expected["late"])

dp_items = (
    spark.table("workspace.gold.gold_delivery_performance")
    .agg(F.sum("total_shipments").alias("items"))
    .collect()[0]["items"]
)
check("delivery_performance statuses partition all order items", dp_items == expected["items"])

# --- Report
failed = [c for c in checks if not c[1]]
for name, ok, detail in checks:
    print(f"{'PASS' if ok else 'FAIL'} - {name}" + (f" ({detail})" if detail and not ok else ""))

assert not failed, f"{len(failed)} validation check(s) FAILED: {[f[0] for f in failed]}"
print(f"\nALL {len(checks)} VALIDATION CHECKS PASSED")