# Databricks notebook source
# # Load credentials from Key Vault
# host = dbutils.secrets.get(scope="telecom_scope", key="neon-host")
# user = dbutils.secrets.get(scope="telecom_scope", key="neon-user")
# password = dbutils.secrets.get(scope="telecom_scope", key="neon-password")

# jdbc_url = f"jdbc:postgresql://{host}/neondb?sslmode=require"

# # Test read
# df = spark.read \
#     .format("jdbc") \
#     .option("url", jdbc_url) \
#     .option("dbtable", "customer_profiles") \
#     .option("user", user) \
#     .option("password", password) \
#     .option("driver", "org.postgresql.Driver") \
#     .load()

# print(f"Connection successful — records: {df.count()}")
# df.display()








from pyspark.sql.functions import *

# ── Secure credentials via Key Vault ──
host     = dbutils.secrets.get(scope="telecom_scope", key="neon-host")
user     = dbutils.secrets.get(scope="telecom_scope", key="neon-user")
password = dbutils.secrets.get(scope="telecom_scope", key="neon-password")

jdbc_url = f"jdbc:postgresql://{host}/neondb?sslmode=require"

# ── Read from Neon PostgreSQL ──
try:
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "customer_profiles") \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    print(f"Successfully read {df.count()} records from Neon PostgreSQL")

except Exception as e:
    print(f"ERROR reading from Neon: {str(e)}")
    raise

# ── Preview raw messy data ──
print("\n=== RAW SCHEMA ===")
df.printSchema()

print("\n=== SAMPLE RAW MESSY DATA ===")
df.show(5, truncate=False)

# ── Add ingestion metadata ──
df = df.withColumn("ingested_at", current_timestamp())
df = df.withColumn("source", lit("NEON_POSTGRESQL"))
df = df.withColumn("pipeline_date", current_date())

# ── Write to Bronze Delta Lake table ──
spark.sql("CREATE DATABASE IF NOT EXISTS telecom_db")

df.write \
  .mode("overwrite") \
  .format("delta") \
  .saveAsTable("telecom_db.bronze_customers")

# ── Verify write ──
verify = spark.table("telecom_db.bronze_customers")

# ── Summary ──
print("\n" + "="*45)
print("BRONZE CUSTOMER INGESTION COMPLETE")
print("="*45)
print(f"Records read from Neon     : {df.count()}")
print(f"Records written to Delta   : {verify.count()}")
print(f"Target table               : telecom_db.bronze_customers")
print("\nOperator Distribution:")
verify.groupBy("operator").count().show()
print("\nPlan Type Distribution (messy):")
verify.groupBy("plan_type").count().show()
print("="*45)