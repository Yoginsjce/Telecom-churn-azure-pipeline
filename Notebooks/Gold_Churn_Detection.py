# Databricks notebook source
# ─────────────────────────────────────────────
# GOLD LAYER — Churn Detection
# Sources: silver_customers, silver_usage, 
#          bronze_weather
# Target: telecom_db.gold_churn_analysis
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

# ── Load Silver Tables ──
customers = spark.table("telecom_db.silver_customers")
usage     = spark.table("telecom_db.silver_usage")
weather   = spark.table("telecom_db.bronze_weather")

print(f"Customers loaded : {customers.count()}")
print(f"Usage loaded     : {usage.count()}")
print(f"Weather loaded   : {weather.count()}")

# ── Aggregate Weather to City Level ──
weather_agg = weather.groupBy("city").agg(
    avg("rainfall_mm").alias("avg_rainfall_30d"),
    max("windspeed_kmh").alias("max_windspeed_30d"),
    first("weather_impact").alias("predominant_weather"),
    first("network_impact_expected").alias("network_impact")
)

# ── Join All Three Sources ──
df = customers.join(usage, ["customer_id"], "left")
df = df.join(weather_agg, ["city"], "left")

print(f"\nJoined records   : {df.count()}")

# ─────────────────────────────────
# CHURN SIGNALS
# ─────────────────────────────────

# Signal 1: Usage Drop
df = df.withColumn("usage_drop_flag",
    when(col("total_call_mins_30d") < 30, 1)
    .otherwise(0))

# Signal 2: High Complaint Frequency
df = df.withColumn("complaint_flag",
    when(col("total_complaints_30d") >= 3, 1)
    .otherwise(0))

# Signal 3: Payment Failures
df = df.withColumn("payment_flag",
    when(col("payment_failures_last_3months") >= 2, 1)
    .otherwise(0))

# Signal 4: Plan Downgrade
df = df.withColumn("downgrade_flag",
    when(col("plan_downgrade_flag") == "YES", 1)
    .otherwise(0))

# Signal 5: Inactivity
df = df.withColumn("inactivity_flag",
    when(col("days_since_recharge") > 20, 1)
    .otherwise(0))

# Signal 6: Low Active Days
df = df.withColumn("low_activity_flag",
    when(col("active_days_30d") < 10, 1)
    .otherwise(0))

# ─────────────────────────────────
# CHURN SCORE
# ─────────────────────────────────
df = df.withColumn("churn_score",
    col("usage_drop_flag") +
    col("complaint_flag") +
    col("payment_flag") +
    col("downgrade_flag") +
    col("inactivity_flag") +
    col("low_activity_flag"))

# ─────────────────────────────────
# INITIAL RISK CLASSIFICATION
# ─────────────────────────────────
df = df.withColumn("churn_risk",
    when(col("churn_score") >= 4, "HIGH")
    .when(col("churn_score") >= 2, "MEDIUM")
    .otherwise("LOW"))

# ─────────────────────────────────
# WEATHER INTELLIGENCE ADJUSTMENT
# Downgrade risk if complaints are
# likely due to bad weather
# ─────────────────────────────────
df = df.withColumn("churn_risk",
    when(
        (col("churn_risk") == "HIGH") &
        (col("predominant_weather") == "SEVERE") &
        (col("complaint_flag") == 1),
        "MEDIUM")
    .otherwise(col("churn_risk")))

# ─────────────────────────────────
# RECOMMENDED ACTION
# ─────────────────────────────────
df = df.withColumn("recommended_action",
    when(col("churn_risk") == "HIGH",
        "IMMEDIATE_RETENTION_CALL")
    .when(
        (col("churn_risk") == "MEDIUM") &
        (col("tenure_segment") == "LOYAL"),
        "LOYALTY_DISCOUNT_OFFER")
    .when(col("churn_risk") == "MEDIUM",
        "SEND_RECHARGE_OFFER_SMS")
    .otherwise("NO_ACTION_NEEDED"))

# ─────────────────────────────────
# PIPELINE METADATA
# ─────────────────────────────────
df = df.drop("layer","processed_at")
df = df.withColumn("processed_at", current_timestamp())
df = df.withColumn("pipeline_date", current_date())
df = df.withColumn("pipeline_version", lit("v1.0"))
df = df.withColumn("pipeline_name",
    lit("TELECOM_CHURN_DETECTION"))

# ─────────────────────────────────
# PIPELINE SUMMARY
# ─────────────────────────────────
total  = df.count()
high   = df.filter(col("churn_risk") == "HIGH").count()
medium = df.filter(col("churn_risk") == "MEDIUM").count()
low    = df.filter(col("churn_risk") == "LOW").count()

imm_call = df.filter(
    col("recommended_action") == 
    "IMMEDIATE_RETENTION_CALL").count()
loyalty  = df.filter(
    col("recommended_action") == 
    "LOYALTY_DISCOUNT_OFFER").count()
sms      = df.filter(
    col("recommended_action") == 
    "SEND_RECHARGE_OFFER_SMS").count()

print("\n" + "="*50)
print("   CHURN DETECTION PIPELINE SUMMARY")
print("="*50)
print(f"  Total customers processed  : {total}")
print(f"  High churn risk            : {high}")
print(f"  Medium churn risk          : {medium}")
print(f"  Low churn risk             : {low}")
print("-"*50)
print(f"  Immediate retention calls  : {imm_call}")
print(f"  Loyalty discount offers    : {loyalty}")
print(f"  Recharge offer SMS         : {sms}")
print("="*50)

# ─────────────────────────────────
# WRITE GOLD LAYER
# ─────────────────────────────────
df.write \
  .mode("overwrite") \
  .format("delta") \
  .saveAsTable("telecom_db.gold_churn_analysis")

print(f"\nGold table written successfully")
print(f"Target: telecom_db.gold_churn_analysis")

# ─────────────────────────────────
# WRITE TO NEON POSTGRESQL
# Target 2 — Business reporting layer
# ─────────────────────────────────
print("\nWriting to Neon PostgreSQL...")

# Drop timestamp columns — PostgreSQL
# handles them differently
neon_df = df.drop("processed_at")

write_to_neon(neon_df, "gold_churn_analysis")

print("\n" + "="*50)
print("GOLD LAYER — DUAL WRITE COMPLETE")
print("="*50)
print("  Target 1: telecom_db.gold_churn_analysis")
print("            (Delta Lake — Analytics team)")
print("  Target 2: gold_churn_analysis")
print("            (Neon PostgreSQL — Business team)")
print("="*50)

# ── Verify ──
verify = spark.table("telecom_db.gold_churn_analysis")
print(f"Verified record count: {verify.count()}")

# ── Churn by Operator ──
print("\nChurn Risk by Operator:")
verify.groupBy("operator", "churn_risk") \
      .count() \
      .orderBy("operator", "churn_risk") \
      .show()

# ── Churn by City ──
print("High Risk by City:")
verify.filter(col("churn_risk") == "HIGH") \
      .groupBy("city") \
      .count() \
      .orderBy("count", ascending=False) \
      .show()

