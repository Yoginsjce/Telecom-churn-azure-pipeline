# Databricks notebook source
from pyspark.sql.functions import *

# ── Read Bronze ──
df = spark.table("telecom_db.bronze_usage")

print("=== BRONZE USAGE — BEFORE CLEANING ===")
print(f"Total records           : {df.count()}")
print(f"Duplicate rows          : {df.count() - df.dropDuplicates().count()}")
print(f"Null call_duration_mins : {df.filter(col('call_duration_mins').isNull()).count()}")
print(f"Null customer_id        : {df.filter(col('customer_id').isNull()).count()}")

# ── Step 1: Remove Duplicates ──
before = df.count()
df = df.dropDuplicates(["customer_id", "call_date", 
                         "call_duration_mins", "data_used_gb"])
after = df.count()
print(f"\nDuplicates removed      : {before - after}")

# ── Step 2: Remove Nulls on Critical Columns ──
df = df.filter(col("customer_id").isNotNull())
df = df.filter(col("call_date").isNotNull())

# ── Step 3: Fix Data Types ──
df = df.withColumn("call_date",
    to_date(col("call_date"), "yyyy-MM-dd"))
df = df.withColumn("call_duration_mins",
    col("call_duration_mins").cast("double"))
df = df.withColumn("data_used_gb",
    col("data_used_gb").cast("double"))

# ── Step 4: Handle Null call_duration_mins ──
# Replace nulls with 0 — customer had no calls that day
df = df.withColumn("call_duration_mins",
    when(col("call_duration_mins").isNull(), 0.0)
    .otherwise(col("call_duration_mins")))

# ── Step 5: Data Quality Flags ──
df = df.withColumn("dq_flag",
    when(col("call_duration_mins") < 0, "INVALID_DURATION")
    .when(col("data_used_gb") < 0,      "INVALID_DATA")
    .when(col("sms_count") < 0,         "INVALID_SMS")
    .otherwise("VALID"))

# ── Step 6: Keep Only Valid Records ──
invalid_count = df.filter(col("dq_flag") != "VALID").count()
df = df.filter(col("dq_flag") == "VALID")
print(f"Invalid records removed : {invalid_count}")

# ── Step 7: Standardize network_type ──
df = df.withColumn("network_type", upper(col("network_type")))

# ── Step 8: Aggregate to Customer Level ──
# One row per customer — 30 day summary
usage_agg = df.groupBy("customer_id").agg(
    sum("call_duration_mins").alias("total_call_mins_30d"),
    sum("data_used_gb").alias("total_data_gb_30d"),
    sum("sms_count").alias("total_sms_30d"),
    sum("call_drops").alias("total_call_drops_30d"),
    sum("complaint_raised").alias("total_complaints_30d"),
    sum("roaming_flag").alias("total_roaming_days_30d"),
    countDistinct("call_date").alias("active_days_30d"),
    avg("call_duration_mins").alias("avg_call_duration"),
    max("call_date").alias("last_active_date")
)

# ── Step 9: Add Usage Behavior Flags ──
usage_agg = usage_agg.withColumn("data_usage_tier",
    when(col("total_data_gb_30d") > 50, "HEAVY")
    .when(col("total_data_gb_30d") > 20, "MODERATE")
    .when(col("total_data_gb_30d") > 5,  "LIGHT")
    .otherwise("MINIMAL"))

usage_agg = usage_agg.withColumn("call_behavior",
    when(col("total_call_mins_30d") > 500, "HEAVY_CALLER")
    .when(col("total_call_mins_30d") > 200, "MODERATE_CALLER")
    .when(col("total_call_mins_30d") > 50,  "LIGHT_CALLER")
    .otherwise("INACTIVE"))

# ── Add Metadata ──
usage_agg = usage_agg.withColumn("processed_at", current_timestamp())
usage_agg = usage_agg.withColumn("layer", lit("SILVER"))

# ── Write Silver ──
usage_agg.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("telecom_db.silver_usage")

# ── Summary ──
verify = spark.table("telecom_db.silver_usage")

print("\n" + "="*45)
print("SILVER USAGE CLEANSING COMPLETE")
print("="*45)
print(f"Records written         : {verify.count()}")
print(f"Target table            : telecom_db.silver_usage")
print("\nData Usage Tier Breakdown:")
verify.groupBy("data_usage_tier").count().show()
print("\nCall Behavior Breakdown:")
verify.groupBy("call_behavior").count().show()
print("="*45)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from telecom_db.dim_plan