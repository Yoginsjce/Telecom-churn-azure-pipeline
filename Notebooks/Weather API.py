# Databricks notebook source
import urllib.request
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ── Indian cities with GPS coordinates ──
cities = [
    ("Mumbai",     19.0760,  72.8777),
    ("Delhi",      28.6139,  77.2090),
    ("Bengaluru",  12.9716,  77.5946),
    ("Chennai",    13.0827,  80.2707),
    ("Hyderabad",  17.3850,  78.4867),
    ("Kolkata",    22.5726,  88.3639),
    ("Pune",       18.5204,  73.8567),
    ("Ahmedabad",  23.0225,  72.5714),
    ("Jaipur",     26.9124,  75.7873),
    ("Lucknow",    26.8467,  80.9462),
    ("Surat",      21.1702,  72.8311),
    ("Nagpur",     21.1458,  79.0882),
    ("Indore",     22.7196,  75.8577),
    ("Bhopal",     23.2599,  77.4126),
    ("Patna",      25.5941,  85.1376)
]

weather_records = []

print("Fetching weather data for all cities...")
print("─" * 45)

# ── Hit API for each city ──
for city, lat, lon in cities:
    try:
        url = (
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude={lat}"
            f"&longitude={lon}"
            f"&daily=precipitation_sum,"
            f"windspeed_10m_max,"
            f"temperature_2m_max,"
            f"temperature_2m_min"
            f"&timezone=Asia%2FKolkata"
            f"&past_days=30"
        )

        # Hit the API
        response = urllib.request.urlopen(url) \
                                 .read() \
                                 .decode("utf-8")
        data = json.loads(response)

        # Extract arrays from response
        dates     = data["daily"]["time"]
        rainfall  = data["daily"]["precipitation_sum"]
        windspeed = data["daily"]["windspeed_10m_max"]
        temp_max  = data["daily"]["temperature_2m_max"]
        temp_min  = data["daily"]["temperature_2m_min"]

        # Flatten into rows
        for i in range(len(dates)):
            weather_records.append((
                city,
                dates[i],
                float(rainfall[i])  if rainfall[i]  is not None else 0.0,
                float(windspeed[i]) if windspeed[i] is not None else 0.0,
                float(temp_max[i])  if temp_max[i]  is not None else 0.0,
                float(temp_min[i])  if temp_min[i]  is not None else 0.0
            ))

        print(f" {city:<12} — {len(dates)} days fetched")

    except Exception as e:
        print(f"{city:<12} — ERROR: {str(e)}")

print("─" * 45)
print(f"Total raw records collected: {len(weather_records)}")

# ── Define Schema ──
schema = StructType([
    StructField("city",          StringType(),  True),
    StructField("weather_date",  StringType(),  True),
    StructField("rainfall_mm",   FloatType(),   True),
    StructField("windspeed_kmh", FloatType(),   True),
    StructField("temp_max_c",    FloatType(),   True),
    StructField("temp_min_c",    FloatType(),   True)
])

# ── Create Spark DataFrame ──
df = spark.createDataFrame(weather_records, schema)

# ── Convert date string to proper date type ──
df = df.withColumn("weather_date", 
    to_date(col("weather_date"), "yyyy-MM-dd"))

# ── Weather impact classification ──
df = df.withColumn("weather_impact",
    when(col("rainfall_mm") > 50, "SEVERE")
    .when(col("rainfall_mm") > 20, "MODERATE")
    .when(col("rainfall_mm") > 5,  "LIGHT")
    .otherwise("CLEAR"))

# ── Network impact flag ──
df = df.withColumn("network_impact_expected",
    when(col("weather_impact") == "SEVERE",   "HIGH")
    .when(col("weather_impact") == "MODERATE", "MEDIUM")
    .otherwise("LOW"))

# ── Add metadata ──
df = df.withColumn("ingested_at", current_timestamp())
df = df.withColumn("source", lit("OPEN_METEO_API"))

# ── Preview before writing ──
print("\n=== SCHEMA ===")
df.printSchema()

print("\n=== SAMPLE DATA ===")
df.show(10, truncate=False)

# ── Write to Bronze Delta Lake ──
df.write \
  .mode("overwrite") \
  .format("delta") \
  .saveAsTable("telecom_db.bronze_weather")

# ── Verify ──
verify = spark.table("telecom_db.bronze_weather")

# ── Summary ──
print("\n" + "="*45)
print("BRONZE WEATHER INGESTION COMPLETE")
print("="*45)
print(f"Total records written  : {verify.count()}")
print(f"Cities covered         : {len(cities)}")
print(f"Target table           : telecom_db.bronze_weather")
print(f"Date range             : {verify.agg(min('weather_date')).collect()[0][0]}"
      f" to {verify.agg(max('weather_date')).collect()[0][0]}")
print("\nWeather Impact Breakdown:")
verify.groupBy("weather_impact") \
      .count() \
      .orderBy("count", ascending=False) \
      .show()
print("\nNetwork Impact Breakdown:")
verify.groupBy("network_impact_expected") \
      .count() \
      .orderBy("count", ascending=False) \
      .show()
print("="*45)

# COMMAND ----------

