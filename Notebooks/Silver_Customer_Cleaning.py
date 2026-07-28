# Databricks notebook source
# ─────────────────────────────────────────────
# SILVER LAYER — Customer Profiles Cleansing
# Source: telecom_db.bronze_customers
# Target: telecom_db.silver_customers (Delta Lake)
# ─────────────────────────────────────────────

from pyspark.sql.functions import *
from pyspark.sql.functions import expr 

# ── Read Bronze ──
df = spark.table("telecom_db.bronze_customers")

print("=== BRONZE CUSTOMERS — BEFORE CLEANING ===")
print(f"Total records           : {df.count()}")
print(f"Null city               : {df.filter(col('city').isNull()).count()}")
print(f"Null plan_type          : {df.filter(col('plan_type').isNull()).count()}")
print(f"Null phone_number       : {df.filter(col('phone_number').isNull()).count()}")
print(f"Null gender             : {df.filter(col('gender').isNull()).count()}")
print(f"Null last_recharge_date : {df.filter(col('last_recharge_date').isNull()).count()}")

# ── Step 1: Remove Duplicates ──
before = df.count()
df = df.dropDuplicates(["customer_id"])
print(f"\nDuplicates removed      : {before - df.count()}")

# ── Step 2: Remove Nulls on Critical Columns ──
df = df.filter(col("customer_id").isNotNull())
df = df.filter(col("operator").isNotNull())

# ── Step 3: Standardize City Names ──
# Fix all variations to one standard name
df = df.withColumn("city",
    when(upper(col("city")).isin(
        "BANGALORE", "BENGALURU", "BLR", "BANGLORE"),
        "Bengaluru")
    .when(upper(col("city")).isin(
        "BOMBAY", "MUMBAI", "MUM"),
        "Mumbai")
    .when(upper(col("city")).isin(
        "DELHI", "NEW DELHI", "NCR", "NEW DELHI"),
        "Delhi")
    .when(upper(col("city")).isin(
        "MADRAS", "CHENNAI"),
        "Chennai")
    .when(upper(col("city")).isin(
        "HYDERABAD", "HYD"),
        "Hyderabad")
    .when(upper(col("city")).isin(
        "CALCUTTA", "KOLKATA"),
        "Kolkata")
    .when(upper(col("city")).isin(
        "POONA", "PUNE"),
        "Pune")
    .when(upper(col("city")).isin(
        "AMDAVAD", "AHMEDABAD"),
        "Ahmedabad")
    .when(upper(col("city")).isin(
        "JAIPUR"), "Jaipur")
    .when(upper(col("city")).isin(
        "LUCKNOW"), "Lucknow")
    .when(upper(col("city")).isin(
        "SURAT"), "Surat")
    .when(upper(col("city")).isin(
        "NAGPUR"), "Nagpur")
    .when(upper(col("city")).isin(
        "INDORE"), "Indore")
    .when(upper(col("city")).isin(
        "BHOPAL"), "Bhopal")
    .when(upper(col("city")).isin(
        "PATNA"), "Patna")
    .otherwise(initcap(col("city"))))

# ── Step 4: Standardize Plan Type ──
df = df.withColumn("plan_type",
    when(upper(regexp_replace(
        col("plan_type"), "[^A-Z]", ""))
        .isin("PREPAID", "PREPAID", "PREPAID"),
        "PREPAID")
    .when(upper(regexp_replace(
        col("plan_type"), "[-_ ]", ""))
        .isin("PREPAID", "PRPAID"),
        "PREPAID")
    .when(upper(regexp_replace(
        col("plan_type"), "[-_ ]", ""))
        .contains("PRE"),
        "PREPAID")
    .when(upper(regexp_replace(
        col("plan_type"), "[-_ ]", ""))
        .contains("POST"),
        "POSTPAID")
    .otherwise(None))

# ── Step 5: Standardize Gender ──
df = df.withColumn("gender",
    when(upper(col("gender"))
        .isin("M", "MALE"), "MALE")
    .when(upper(col("gender"))
        .isin("F", "FEMALE"), "FEMALE")
    .otherwise("UNKNOWN"))

# ── Step 6: Standardize Phone Numbers ──
# Strip all formatting — keep digits only
# Then add +91 prefix
df = df.withColumn("phone_clean",
    regexp_replace(col("phone_number"), 
                   "[^0-9]", ""))

df = df.withColumn("phone_clean",
    when(col("phone_clean").isNull(), None)
    .when(length(col("phone_clean")) == 12,
        concat(lit("+"), col("phone_clean")))
    .when(length(col("phone_clean")) == 11,
        concat(lit("+91"), 
               col("phone_clean").substr(2, 10)))
    .when(length(col("phone_clean")) == 10,
        concat(lit("+91"), col("phone_clean")))
    .otherwise(None))

# ── Step 7: Standardize Dates ──
# Try multiple date formats
df = df.withColumn("last_recharge_date_clean",
    coalesce(
        expr("try_to_date(last_recharge_date, 'yyyy-MM-dd')"),
        expr("try_to_date(last_recharge_date, 'dd/MM/yyyy')"),
        expr("try_to_date(last_recharge_date, 'dd-MM-yyyy')"),
        expr("try_to_date(last_recharge_date, 'MM/dd/yyyy')"),
        expr("try_to_date(last_recharge_date, 'dd MMM yyyy')"),
        expr("try_to_date(last_recharge_date, 'yyyyMMdd')")
    ))

df = df.withColumn("join_date_clean",
    coalesce(
        expr("try_to_date(join_date, 'yyyy-MM-dd')"),
        expr("try_to_date(join_date, 'dd/MM/yyyy')"),
        expr("try_to_date(join_date, 'dd-MM-yyyy')"),
        expr("try_to_date(join_date, 'MM/dd/yyyy')"),
        expr("try_to_date(join_date, 'dd MMM yyyy')"),
        expr("try_to_date(join_date, 'yyyyMMdd')")
    ))

# ── Step 8: Days Since Last Recharge ──
df = df.withColumn("days_since_recharge",
    datediff(current_date(), 
             col("last_recharge_date_clean")))

# ── Step 9: Fix Age ──
df = df.withColumn("age",
    col("age").cast("double").cast("integer"))
    
df = df.withColumn("age",
    when((col("age") < 18) | 
         (col("age") > 90), None)
    .otherwise(col("age")))

# ── Step 10: Standardize Plan Downgrade Flag ──
df = df.withColumn("plan_downgrade_flag",
    when(upper(col("plan_downgrade_flag"))
        .isin("Y", "YES", "1", "TRUE"), "YES")
    .otherwise("NO"))

# ── Step 11: Fix Plan Value ──
df = df.withColumn("plan_value_inr",
    col("plan_value_inr").cast("double"))

# ── Step 12: Tenure Segmentation ──
df = df.withColumn("tenure_segment",
    when(col("tenure_months") > 36, "LOYAL")
    .when(col("tenure_months") > 12, "ESTABLISHED")
    .when(col("tenure_months") > 6,  "GROWING")
    .otherwise("NEW"))

# ── Step 13: Customer Value Segment ──
df = df.withColumn("customer_value_segment",
    when(col("plan_value_inr") > 999,  "PREMIUM")
    .when(col("plan_value_inr") > 499, "HIGH")
    .when(col("plan_value_inr") > 199, "MEDIUM")
    .otherwise("LOW"))

# ── Step 14: Add Metadata ──
df = df.withColumn("processed_at", current_timestamp())
df = df.withColumn("layer", lit("SILVER"))

# ── Select Final Clean Columns ──
silver_df = df.select(
    "customer_id",
    "customer_name",
    "age",
    col ("phone_clean").alias("phone_number"),
    "city",
    "state",
    "operator",
    "plan_type",
    "plan_value_inr",
    "tenure_months",
    "tenure_segment",
    "customer_value_segment",
    col("join_date_clean").alias("join_date"),
    col("last_recharge_date_clean").alias("last_recharge_date"),
    "days_since_recharge",
    "payment_failures_last_3months",
    "plan_downgrade_flag",
    "gender",
    "email",
    "processed_at",
    "layer"
)

# ── Write Silver ──
silver_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("telecom_db.silver_customers")

# ── Summary ──
verify = spark.table("telecom_db.silver_customers")

print("\n" + "="*45)
print("SILVER CUSTOMER CLEANSING COMPLETE")
print("="*45)
print(f"Records written              : {verify.count()}")
print(f"Target table                 : telecom_db.silver_customers")
print("\nCity Distribution (cleaned):")
verify.groupBy("city").count() \
      .orderBy("count", ascending=False).show()
print("\nPlan Type Distribution (cleaned):")
verify.groupBy("plan_type").count().show()
print("\nGender Distribution (cleaned):")
verify.groupBy("gender").count().show()
print("\nTenure Segment Distribution:")
verify.groupBy("tenure_segment").count().show()
print("\nCustomer Value Segment:")
verify.groupBy("customer_value_segment") \
      .count().show()
print("\nNull Check After Cleaning:")
print(f"Null city               : {verify.filter(col('city').isNull()).count()}")
print(f"Null plan_type          : {verify.filter(col('plan_type').isNull()).count()}")
print(f"Null phone_number       : {verify.filter(col('phone_number').isNull()).count()}")
print(f"Null last_recharge_date : {verify.filter(col('last_recharge_date').isNull()).count()}")
print("="*45)