# Databricks notebook source
# ─────────────────────────────────────────────
# BRONZE LAYER — Usage Records from ADLS
# Source: usage_records.parquet
# Target: telecom_db.bronze_usage (Delta Lake)
# ─────────────────────────────────────────────

from pyspark.sql.functions import *

# ── Secure ADLS access via Key Vault ──
storage_key = dbutils.secrets.get(
    scope="telecom_adl_scope", 
    key="Adl-strg-key")

storage_account = "telecomadl"

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.blob.core.windows.net",
    storage_key)

# ── Read parquet from ADLS ──
blob_path = f"wasbs://telecom-adl-con@{storage_account}.blob.core.windows.net/usage/usage_records.parquet"

try:
    df = spark.read.parquet(blob_path)
    print(f"Successfully read {df.count()} records from ADLS")
except Exception as e:
    print(f"ERROR reading from ADLS: {str(e)}")
    raise

# ── Preview raw data ──
print("\n=== RAW SCHEMA ===")
df.printSchema()

print("\n=== SAMPLE RAW DATA ===")
df.show(5, truncate=False)

# ── Add ingestion metadata ──
df = df.withColumn("ingested_at", current_timestamp())
df = df.withColumn("source", lit("ADLS_USAGE"))
df = df.withColumn("pipeline_date", current_date())

# ── Write to Bronze Delta Lake table ──
spark.sql("CREATE DATABASE IF NOT EXISTS telecom_db")

df.write \
  .mode("overwrite") \
  .format("delta") \
  .saveAsTable("telecom_db.bronze_usage")

# ── Summary ──
print("\n" + "="*45)
print("BRONZE USAGE INGESTION COMPLETE")
print("="*45)
print(f"Total records written : {df.count()}")
print(f"Columns               : {len(df.columns)}")
print(f"Target table          : telecom_db.bronze_usage")
print("="*45)