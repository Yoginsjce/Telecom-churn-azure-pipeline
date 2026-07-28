# Telecom-churn-azure-pipeline
End-to-end Azure Data Engineering  pipeline for telecom customer churn detection  using Databricks, ADF, Delta Lake and Power BI


# 🚀 Telecom Customer Churn Detection Pipeline
### End-to-End Azure Data Engineering Project

![Azure](https://img.shields.io/badge/Azure-Databricks-orange)
![PySpark](https://img.shields.io/badge/PySpark-3.x-red)
![Delta Lake](https://img.shields.io/badge/Delta-Lake-blue)
![Power BI](https://img.shields.io/badge/Power-BI-yellow)
![ADF](https://img.shields.io/badge/Azure-Data%20Factory-green)

---

## 📌 Problem Statement

Telecom companies like Airtel and Jio lose crores 
monthly to customer churn. By the time they detect 
a churned customer, it is already too late to act. 
This pipeline identifies customers likely to churn 
BEFORE they leave — giving retention teams a daily 
prioritized action list.

---

## 🏗️ Architecture

[INSERT ARCHITECTURE DIAGRAM IMAGE HERE]

---

## 📊 Tech Stack

| Layer | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Processing | Azure Databricks, PySpark, Spark SQL |
| Orchestration | Azure Data Factory |
| Storage | Azure Data Lake Storage, Delta Lake |
| Source Database | Neon PostgreSQL |
| External API | Open-Meteo Weather API |
| Security | Azure Key Vault |
| Data Model | Star Schema |
| Visualization | Power BI |

---

## 🗂️ Data Sources

| Source | Type | Records | Content |
|---|---|---|---|
| ADLS | Parquet | 91,799 | Call records, usage data |
| Neon PostgreSQL | JDBC | 5,000 | Customer profiles (messy) |
| Open-Meteo API | REST | 555 | 30-day weather data |

---

## 🔄 Pipeline Layers

### Bronze Layer — Raw Ingestion
- Ingests raw data from all 3 sources as-is
- No transformations applied
- Adds ingestion metadata (timestamp, source)
- Writes to Delta Lake bronze tables

### Silver Layer — Cleansing & Standardization
Fixes 8 categories of data quality issues:
- City name inconsistencies (Bengaluru/Bangalore/BLR)
- 6 different date formats standardized
- Phone number format normalization
- Mixed case gender values
- Plan type inconsistencies
- Invalid age values
- Duplicate records
- Null value handling

### Gold Layer — Business Logic
6-signal churn scoring model:

| Signal | Condition | Flag |
|---|---|---|
| Usage Drop | Call mins < 30 in 30 days | 1 |
| Complaint Frequency | 3+ complaints in 30 days | 1 |
| Payment Failures | 2+ failures in 3 months | 1 |
| Plan Downgrade | Downgraded to lower plan | 1 |
| Inactivity | No recharge in 20+ days | 1 |
| Low Active Days | Active < 10 days in 30 | 1 |

Churn Score = Sum of all flags (0-6)
- Score 4-6 → HIGH RISK
- Score 2-3 → MEDIUM RISK
- Score 0-1 → LOW RISK

**Weather Intelligence:**
Customers with call drop complaints during 
SEVERE weather events are downgraded from 
HIGH to MEDIUM — avoiding false positive churn flags.

### Data Model Layer — Star Schema
fact_churn (centre)
│
├── dim_customer
├── dim_plan
├── dim_usage
└── dim_weather

---

## 🎯 Output

| Target | Type | Tables | Consumers |
|---|---|---|---|
| Delta Lake | Analytical | 10 tables | Power BI |
| Neon PostgreSQL | Operational | 6 tables | Business team |

---

## 📈 Power BI Dashboard

5 dashboard pages:
1. Churn Overview — Risk distribution and KPIs
2. Customer Analysis — City heatmap and segments
3. Usage Patterns — Behavioral correlation
4. Weather Impact — Environmental context
5. Retention Actions — Prioritized action list

[INSERT DASHBOARD SCREENSHOT]

---

## 🔒 Security

- All credentials stored in Azure Key Vault
- Databricks secret scope for secure access
- No hardcoded values anywhere in codebase
- Neon PostgreSQL SSL mode required

---

## ⚙️ ADF Orchestration

Pipeline execution order: Bronze (parallel) → Silver (parallel)→ Gold → Star Schema

Scheduled daily at 9AM IST via ADF trigger.

---

## 🚀 How To Run

1. Clone this repository
2. Upload usage_records.parquet to ADLS
3. Load customer_profiles_messy.csv to PostgreSQL
4. Configure Azure Key Vault secrets
5. Import notebooks to Databricks
6. Import ADF pipeline JSON
7. Trigger pipeline

---

## 📸 Screenshots

### ADF Pipeline
[INSERT ADF SCREENSHOT]

### Databricks Tables
[]

### Power BI Dashboard
[INSERT POWER BI SCREENSHOT]

---

## 👨‍💻 Author

**Yogin N C**
Azure Data Engineer
📧 yoginnc@gmail.com
🔗 https://www.linkedin.com/in/yogin-nc-2b006324b/
🐙 https://github.com/Yoginsjce?tab=repositories
