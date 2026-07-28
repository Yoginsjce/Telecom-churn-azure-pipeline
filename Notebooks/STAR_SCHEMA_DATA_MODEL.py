# Databricks notebook source
# ─────────────────────────────────────────────
# DATA MODEL LAYER — Star Schema
# Source: telecom_db.gold_churn_analysis
#         telecom_db.bronze_weather
# Targets: dim_customer, dim_plan, 
#          dim_usage, dim_weather, fact_churn
# ─────────────────────────────────────────────

from pyspark.sql.functions import *

# ── Neon PostgreSQL Write Helper ──
def write_to_neon(df, table_name):
    """
    Writes a Spark DataFrame to Neon PostgreSQL
    via JDBC connection
    """
    host     = dbutils.secrets.get(
        scope="telecom_scope", key="neon-host")
    user     = dbutils.secrets.get(
        scope="telecom_scope", key="neon-user")
    password = dbutils.secrets.get(
        scope="telecom_scope", key="neon-password")

    jdbc_url = (
        f"jdbc:postgresql://{host}"
        f"/neondb?sslmode=require"
    )

    try:
        df.write \
          .format("jdbc") \
          .option("url", jdbc_url) \
          .option("dbtable", table_name) \
          .option("user", user) \
          .option("password", password) \
          .option("driver", 
                  "org.postgresql.Driver") \
          .mode("overwrite") \
          .save()
        print(f"  ✓ {table_name} → Neon PostgreSQL")

    except Exception as e:
        print(f"  ✗ {table_name} → Neon FAILED: {str(e)}")
        raise

# ── Load Gold Table ──
gold = spark.table("telecom_db.gold_churn_analysis")

print(f"Gold records loaded: {gold.count()}")
print("\nGold columns available:")
print(gold.columns)

# ════════════════════════════════════════
# DIM_CUSTOMER
# Who the customer is
# ════════════════════════════════════════
dim_customer = gold.select(
    "customer_id",
    "customer_name",
    "age",
    "phone_number",
    "city",
    "state",
    "gender",
    "operator",
    "tenure_months",
    "tenure_segment",
    "customer_value_segment",
    "days_since_recharge",
    "payment_failures_last_3months",
    "plan_downgrade_flag",
    "churn_risk",
    "recommended_action"
).dropDuplicates(["customer_id"])

# Add surrogate key
dim_customer = dim_customer.withColumn(
    "customer_sk",
    monotonically_increasing_id())

# SCD Type 1 metadata
dim_customer = dim_customer \
    .withColumn("is_current", lit("Y")) \
    .withColumn("effective_start_date", current_date()) \
    .withColumn("effective_end_date",
        lit(None).cast("date")) \
    .withColumn("created_at", current_timestamp())

dim_customer.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("telecom_db.dim_customer")

print(f"\ndim_customer written : {dim_customer.count()} rows")

# ════════════════════════════════════════
# DIM_PLAN
# What plan the customer has
# ════════════════════════════════════════
dim_plan = gold.select(
    "plan_type",
    "plan_value_inr"
).dropDuplicates(["plan_type", "plan_value_inr"])

# Filter nulls
dim_plan = dim_plan.filter(
    col("plan_type").isNotNull())

# Add plan category
dim_plan = dim_plan.withColumn("plan_category",
    when(col("plan_value_inr") > 999,  "PREMIUM")
    .when(col("plan_value_inr") > 499, "MID_RANGE")
    .when(col("plan_value_inr") > 199, "BUDGET")
    .otherwise("BASIC"))

# Add surrogate key
dim_plan = dim_plan.withColumn(
    "plan_sk",
    monotonically_increasing_id())

dim_plan = dim_plan \
    .withColumn("created_at", current_timestamp())

dim_plan.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("telecom_db.dim_plan")

print(f"dim_plan written     : {dim_plan.count()} rows")

# ════════════════════════════════════════
# DIM_USAGE
# How the customer used the service
# ════════════════════════════════════════
dim_usage = gold.select(
    "customer_id",
    "total_call_mins_30d",
    "total_data_gb_30d",
    "total_sms_30d",
    "total_call_drops_30d",
    "total_complaints_30d",
    "active_days_30d",
    "data_usage_tier",
    "call_behavior"
).dropDuplicates(["customer_id"])

# Add surrogate key
dim_usage = dim_usage.withColumn(
    "usage_sk",
    monotonically_increasing_id())

dim_usage = dim_usage \
    .withColumn("created_at", current_timestamp())

dim_usage.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("telecom_db.dim_usage")

print(f"dim_usage written    : {dim_usage.count()} rows")

# ════════════════════════════════════════
# DIM_WEATHER
# Environmental context by city
# ════════════════════════════════════════
weather = spark.table("telecom_db.bronze_weather")

dim_weather = weather.groupBy("city").agg(
    avg("rainfall_mm").alias("avg_rainfall_30d"),
    max("windspeed_kmh").alias("max_windspeed_30d"),
    first("weather_impact").alias("predominant_weather"),
    first("network_impact_expected").alias("network_impact")
)

# Add surrogate key
dim_weather = dim_weather.withColumn(
    "weather_sk",
    monotonically_increasing_id())

dim_weather = dim_weather \
    .withColumn("created_at", current_timestamp())

dim_weather.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("telecom_db.dim_weather")

print(f"dim_weather written  : {dim_weather.count()} rows")

# ════════════════════════════════════════
# FACT_CHURN
# Central fact table — churn measurements
# ════════════════════════════════════════

# Join gold with dimension surrogate keys
fact = gold.join(
    dim_customer.select(
        "customer_id", "customer_sk"),
    ["customer_id"], "left")

fact = fact.join(
    dim_plan.select(
        "plan_type", "plan_value_inr", "plan_sk"),
    ["plan_type", "plan_value_inr"], "left")

fact = fact.join(
    dim_usage.select(
        "customer_id", "usage_sk"),
    ["customer_id"], "left")

fact = fact.join(
    dim_weather.select(
        "city", "weather_sk"),
    ["city"], "left")

# Select only fact columns
fact_churn = fact.select(
    monotonically_increasing_id().alias("churn_event_id"),
    "customer_sk",
    "plan_sk",
    "usage_sk",
    "weather_sk",
    "usage_drop_flag",
    "complaint_flag",
    "payment_flag",
    "downgrade_flag",
    "inactivity_flag",
    "low_activity_flag",
    "churn_score",
    current_date().alias("pipeline_date"),
    current_timestamp().alias("processed_at")
)

fact_churn.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("telecom_db.fact_churn")

print(f"fact_churn written   : {fact_churn.count()} rows")

# ── Final Summary ──
print("\n" + "="*50)
print("   STAR SCHEMA DATA MODEL COMPLETE")
print("="*50)
print(f"  dim_customer  : {spark.table('telecom_db.dim_customer').count()} rows")
print(f"  dim_plan      : {spark.table('telecom_db.dim_plan').count()} rows")
print(f"  dim_usage     : {spark.table('telecom_db.dim_usage').count()} rows")
print(f"  dim_weather   : {spark.table('telecom_db.dim_weather').count()} rows")
print(f"  fact_churn    : {spark.table('telecom_db.fact_churn').count()} rows")
print("="*50)


# ─────────────────────────────────
# WRITE STAR SCHEMA TO NEON
# Target 2 — Business reporting layer
# ─────────────────────────────────
print("\nWriting Star Schema to Neon PostgreSQL...")
print("─"*50)

# Write each dimension and fact table
write_to_neon(
    dim_customer.drop("created_at"),
    "dim_customer")

write_to_neon(
    dim_plan.drop("created_at"),
    "dim_plan")

write_to_neon(
    dim_usage.drop("created_at"),
    "dim_usage")

write_to_neon(
    dim_weather.drop("created_at"),
    "dim_weather")

write_to_neon(
    fact_churn.drop("processed_at"),
    "fact_churn")

print("─"*50)
print("\n" + "="*50)
print("STAR SCHEMA — DUAL WRITE COMPLETE")
print("="*50)
print("  Delta Lake targets:")
print("    telecom_db.dim_customer")
print("    telecom_db.dim_plan")
print("    telecom_db.dim_usage")
print("    telecom_db.dim_weather")
print("    telecom_db.fact_churn")
print("\n  Neon PostgreSQL targets:")
print("    dim_customer")
print("    dim_plan")
print("    dim_usage")
print("    dim_weather")
print("    fact_churn")
print("="*50)
# print("\nReady for Power BI Connection")
# print("Tables to connect in Power BI:")
# print("  telecom_db.dim_customer")
# print("  telecom_db.dim_plan")
# print("  telecom_db.dim_usage")
# print("  telecom_db.dim_weather")
# print("  telecom_db.fact_churn")
# print("="*50)