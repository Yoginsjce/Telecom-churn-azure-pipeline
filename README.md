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

#
<style>
@keyframes flowRight { 0%{stroke-dashoffset:24}100%{stroke-dashoffset:0} }
@keyframes flowDown  { 0%{stroke-dashoffset:24}100%{stroke-dashoffset:0} }
@keyframes pulse     { 0%,100%{opacity:1}50%{opacity:.6} }
@keyframes fadeIn    { from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)} }
.flow-h { stroke-dasharray:6 4; animation: flowRight 1.2s linear infinite; }
.flow-v { stroke-dasharray:6 4; animation: flowDown  1.2s linear infinite; }
.pulse  { animation: pulse 2.4s ease-in-out infinite; }
.card   { animation: fadeIn .5s ease both; }
.card:nth-child(1){animation-delay:.05s}.card:nth-child(2){animation-delay:.1s}
.card:nth-child(3){animation-delay:.15s}.card:nth-child(4){animation-delay:.2s}
.card:nth-child(5){animation-delay:.25s}.card:nth-child(6){animation-delay:.3s}
.card:nth-child(7){animation-delay:.35s}.card:nth-child(8){animation-delay:.4s}
</style>

<svg width="100%" viewBox="0 0 960 720" role="img" xmlns="http://www.w3.org/2000/svg">
<title>Telecom Churn Detection — Azure Data Pipeline Architecture</title>
<desc>End-to-end data pipeline from 3 sources through medallion architecture to dual targets and Power BI</desc>
<defs>
  <marker id="arr" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
    <path d="M2 1L8 5L2 9" fill="none" stroke="context-stroke" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
  </marker>

  <!-- ADF icon — factory/pipeline shape -->
  <symbol id="ico-adf" viewBox="0 0 32 32">
    <rect x="2" y="8" width="28" height="18" rx="3" fill="#0078D4" opacity=".15"/>
    <rect x="2" y="8" width="28" height="18" rx="3" fill="none" stroke="#0078D4" stroke-width="1.5"/>
    <path d="M8 20L12 12L16 17L20 13L24 20" stroke="#0078D4" stroke-width="1.8" fill="none" stroke-linecap="round" stroke-linejoin="round"/>
    <circle cx="8"  cy="20" r="1.5" fill="#0078D4"/>
    <circle cx="16" cy="17" r="1.5" fill="#0078D4"/>
    <circle cx="24" cy="20" r="1.5" fill="#0078D4"/>
    <rect x="10" y="4" width="12" height="5" rx="1.5" fill="#0078D4" opacity=".6"/>
    <path d="M16 4V2" stroke="#0078D4" stroke-width="1.5" stroke-linecap="round"/>
  </symbol>

  <!-- Databricks icon — stylized spark/diamond -->
  <symbol id="ico-adb" viewBox="0 0 32 32">
    <path d="M16 2L28 9V23L16 30L4 23V9Z" fill="#FF3621" opacity=".12"/>
    <path d="M16 2L28 9V23L16 30L4 23V9Z" fill="none" stroke="#FF3621" stroke-width="1.5"/>
    <path d="M10 16L16 12L22 16L16 20Z" fill="#FF3621" opacity=".8"/>
    <path d="M16 12V8M16 20V24" stroke="#FF3621" stroke-width="1.5" stroke-linecap="round"/>
    <path d="M10 16L7 14M22 16L25 14" stroke="#FF3621" stroke-width="1.2" stroke-linecap="round" opacity=".6"/>
  </symbol>

  <!-- ADLS icon — storage layers -->
  <symbol id="ico-adls" viewBox="0 0 32 32">
    <ellipse cx="16" cy="8"  rx="12" ry="4" fill="#0078D4" opacity=".2"/>
    <ellipse cx="16" cy="8"  rx="12" ry="4" fill="none" stroke="#0078D4" stroke-width="1.5"/>
    <path d="M4 8V16M28 8V16" stroke="#0078D4" stroke-width="1.5"/>
    <ellipse cx="16" cy="16" rx="12" ry="4" fill="#0078D4" opacity=".2"/>
    <ellipse cx="16" cy="16" rx="12" ry="4" fill="none" stroke="#0078D4" stroke-width="1.5"/>
    <path d="M4 16V24M28 16V24" stroke="#0078D4" stroke-width="1.5"/>
    <ellipse cx="16" cy="24" rx="12" ry="4" fill="#0078D4" opacity=".2"/>
    <ellipse cx="16" cy="24" rx="12" ry="4" fill="none" stroke="#0078D4" stroke-width="1.5"/>
  </symbol>

  <!-- Key Vault icon — shield with key -->
  <symbol id="ico-kv" viewBox="0 0 32 32">
    <path d="M16 2L28 7V18C28 24 22 29 16 30C10 29 4 24 4 18V7Z" fill="#FFB900" opacity=".15"/>
    <path d="M16 2L28 7V18C28 24 22 29 16 30C10 29 4 24 4 18V7Z" fill="none" stroke="#FFB900" stroke-width="1.5"/>
    <circle cx="16" cy="15" r="4" fill="none" stroke="#FFB900" stroke-width="1.5"/>
    <path d="M16 19V24M14 22H18" stroke="#FFB900" stroke-width="1.5" stroke-linecap="round"/>
  </symbol>

  <!-- PostgreSQL icon — elephant stylized -->
  <symbol id="ico-pg" viewBox="0 0 32 32">
    <circle cx="16" cy="16" r="13" fill="#336791" opacity=".12"/>
    <circle cx="16" cy="16" r="13" fill="none" stroke="#336791" stroke-width="1.5"/>
    <path d="M11 10C11 10 10 8 12 7C14 6 17 8 17 10" fill="none" stroke="#336791" stroke-width="1.4" stroke-linecap="round"/>
    <path d="M11 10V20C11 22 13 24 16 24C19 24 21 22 21 20V10C21 8 19 6 16 6" fill="none" stroke="#336791" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"/>
    <path d="M21 12C23 11 25 13 24 16" fill="none" stroke="#336791" stroke-width="1.4" stroke-linecap="round"/>
    <circle cx="14" cy="14" r="1" fill="#336791"/>
    <circle cx="18" cy="14" r="1" fill="#336791"/>
  </symbol>

  <!-- API / Globe icon -->
  <symbol id="ico-api" viewBox="0 0 32 32">
    <circle cx="16" cy="16" r="13" fill="#00B4D8" opacity=".12"/>
    <circle cx="16" cy="16" r="13" fill="none" stroke="#00B4D8" stroke-width="1.5"/>
    <ellipse cx="16" cy="16" rx="6" ry="13" fill="none" stroke="#00B4D8" stroke-width="1.2"/>
    <line x1="3" y1="16" x2="29" y2="16" stroke="#00B4D8" stroke-width="1.2"/>
    <path d="M5 10C8 11 12 12 16 12S24 11 27 10" fill="none" stroke="#00B4D8" stroke-width="1"/>
    <path d="M5 22C8 21 12 20 16 20S24 21 27 22" fill="none" stroke="#00B4D8" stroke-width="1"/>
  </symbol>

  <!-- Delta Lake icon -->
  <symbol id="ico-delta" viewBox="0 0 32 32">
    <path d="M16 3L29 26H3Z" fill="#0078D4" opacity=".12"/>
    <path d="M16 3L29 26H3Z" fill="none" stroke="#0078D4" stroke-width="1.5" stroke-linejoin="round"/>
    <path d="M10 20L16 9L22 20" fill="none" stroke="#0078D4" stroke-width="1.2"/>
    <line x1="9" y1="23" x2="23" y2="23" stroke="#0078D4" stroke-width="1.5" stroke-linecap="round"/>
  </symbol>

  <!-- Power BI icon -->
  <symbol id="ico-pbi" viewBox="0 0 32 32">
    <rect x="4"  y="16" width="6" height="12" rx="2" fill="#F2C811" opacity=".9"/>
    <rect x="13" y="10" width="6" height="18" rx="2" fill="#F2C811"/>
    <rect x="22" y="4"  width="6" height="24" rx="2" fill="#F2C811" opacity=".7"/>
    <path d="M7 14V6C7 5 8 4 9 4H24" stroke="#F2C811" stroke-width="1.2" fill="none" stroke-linecap="round" opacity=".4"/>
  </symbol>

  <!-- Star Schema icon -->
  <symbol id="ico-star" viewBox="0 0 32 32">
    <circle cx="16" cy="16" r="5" fill="#7B61FF" opacity=".3"/>
    <circle cx="16" cy="16" r="5" fill="none" stroke="#7B61FF" stroke-width="1.5"/>
    <circle cx="16" cy="5"  r="3" fill="#7B61FF" opacity=".5"/>
    <circle cx="27" cy="16" r="3" fill="#7B61FF" opacity=".5"/>
    <circle cx="16" cy="27" r="3" fill="#7B61FF" opacity=".5"/>
    <circle cx="5"  cy="16" r="3" fill="#7B61FF" opacity=".5"/>
    <line x1="16" y1="8"  x2="16" y2="11" stroke="#7B61FF" stroke-width="1.2"/>
    <line x1="24" y1="16" x2="21" y2="16" stroke="#7B61FF" stroke-width="1.2"/>
    <line x1="16" y1="24" x2="16" y2="21" stroke="#7B61FF" stroke-width="1.2"/>
    <line x1="8"  y1="16" x2="11" y2="16" stroke="#7B61FF" stroke-width="1.2"/>
  </symbol>
</defs>

<!-- ══════════════════════════════════════════════════════
     TITLE
══════════════════════════════════════════════════════ -->
<text x="480" y="30" text-anchor="middle" font-family="system-ui,sans-serif" font-size="15" font-weight="600" fill="var(--text-primary, #1a1a1a)">Telecom Churn Detection — Azure Data Pipeline</text>
<text x="480" y="48" text-anchor="middle" font-family="system-ui,sans-serif" font-size="11" fill="var(--text-secondary, #666)">Medallion Architecture · ADF · Databricks · Delta Lake · Power BI</text>

<!-- ══════════════════════════════════════════════════════
     ZONE BACKGROUNDS
══════════════════════════════════════════════════════ -->
<!-- Sources zone -->
<rect x="20" y="62" width="150" height="290" rx="12" fill="none" stroke="var(--border,#e0e0e0)" stroke-width="1" stroke-dasharray="5 3"/>
<text x="95" y="80" text-anchor="middle" font-family="system-ui,sans-serif" font-size="10" font-weight="600" fill="var(--text-secondary,#888)" letter-spacing="1">SOURCES</text>

<!-- Bronze zone -->
<rect x="200" y="62" width="140" height="290" rx="12" fill="#CD7F3208" stroke="#CD7F32" stroke-width="1" stroke-dasharray="5 3"/>
<text x="270" y="80" text-anchor="middle" font-family="system-ui,sans-serif" font-size="10" font-weight="600" fill="#CD7F32" letter-spacing="1">BRONZE</text>

<!-- Silver zone -->
<rect x="365" y="62" width="140" height="290" rx="12" fill="#C0C0C008" stroke="#888" stroke-width="1" stroke-dasharray="5 3"/>
<text x="435" y="80" text-anchor="middle" font-family="system-ui,sans-serif" font-size="10" font-weight="600" fill="#888" letter-spacing="1">SILVER</text>

<!-- Gold zone -->
<rect x="530" y="62" width="140" height="290" rx="12" fill="#FFD70008" stroke="#DAA520" stroke-width="1" stroke-dasharray="5 3"/>
<text x="600" y="80" text-anchor="middle" font-family="system-ui,sans-serif" font-size="10" font-weight="600" fill="#DAA520" letter-spacing="1">GOLD</text>

<!-- Targets zone -->
<rect x="695" y="62" width="250" height="290" rx="12" fill="none" stroke="var(--border,#e0e0e0)" stroke-width="1" stroke-dasharray="5 3"/>
<text x="820" y="80" text-anchor="middle" font-family="system-ui,sans-serif" font-size="10" font-weight="600" fill="var(--text-secondary,#888)" letter-spacing="1">TARGETS &amp; CONSUMPTION</text>

<!-- ══════════════════════════════════════════════════════
     KEY VAULT (top centre — spans pipeline)
══════════════════════════════════════════════════════ -->
<g class="card">
  <rect x="390" y="370" width="170" height="56" rx="10" fill="var(--surface-1,#fff)" stroke="#FFB900" stroke-width="1.5"/>
  <use href="#ico-kv" x="402" y="380" width="26" height="26"/>
  <text x="436" y="390" font-family="system-ui,sans-serif" font-size="11" font-weight="600" fill="#B8860B">Azure Key Vault</text>
  <text x="436" y="404" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">Secrets · Credentials</text>
  <line x1="475" y1="370" x2="475" y2="355" stroke="#FFB900" stroke-width="1" stroke-dasharray="3 3" opacity=".7"/>
</g>

<!-- ══════════════════════════════════════════════════════
     ADF ORCHESTRATOR (bottom centre)
══════════════════════════════════════════════════════ -->
<g class="card">
  <rect x="340" y="446" width="280" height="60" rx="10" fill="var(--surface-1,#fff)" stroke="#0078D4" stroke-width="1.5"/>
  <use href="#ico-adf" x="354" y="458" width="28" height="28"/>
  <text x="392" y="470" font-family="system-ui,sans-serif" font-size="12" font-weight="600" fill="#0050A0">Azure Data Factory</text>
  <text x="392" y="485" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">Pipeline orchestration · Daily 9AM IST trigger · Dependency chains</text>
  <!-- ADF connector lines going up to each layer -->
  <line x1="480" y1="446" x2="480" y2="430" stroke="#0078D4" stroke-width="1" stroke-dasharray="3 3" opacity=".5"/>
</g>

<!-- ADF bracket line connecting to zones -->
<path d="M270 440 L270 446 L600 446 L600 440" fill="none" stroke="#0078D4" stroke-width="1" stroke-dasharray="3 3" opacity=".4"/>

<!-- ══════════════════════════════════════════════════════
     SOURCE CARDS
══════════════════════════════════════════════════════ -->
<!-- ADLS Source -->
<g class="card">
  <rect x="28" y="96" width="130" height="66" rx="10" fill="var(--surface-1,#fff)" stroke="#0078D4" stroke-width="1.5"/>
  <use href="#ico-adls" x="36" y="104" width="26" height="26"/>
  <text x="68" y="112" font-family="system-ui,sans-serif" font-size="11" font-weight="600" fill="#0050A0">ADLS</text>
  <text x="68" y="125" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">Parquet</text>
  <text x="36" y="152" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">91,799 usage records</text>
</g>

<!-- PostgreSQL Source -->
<g class="card">
  <rect x="28" y="190" width="130" height="66" rx="10" fill="var(--surface-1,#fff)" stroke="#336791" stroke-width="1.5"/>
  <use href="#ico-pg" x="36" y="198" width="26" height="26"/>
  <text x="68" y="206" font-family="system-ui,sans-serif" font-size="11" font-weight="600" fill="#1a4a6b">Neon PostgreSQL</text>
  <text x="68" y="219" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">JDBC</text>
  <text x="36" y="246" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">5,000 customer profiles</text>
</g>

<!-- WebAPI Source -->
<g class="card">
  <rect x="28" y="284" width="130" height="66" rx="10" fill="var(--surface-1,#fff)" stroke="#00B4D8" stroke-width="1.5"/>
  <use href="#ico-api" x="36" y="292" width="26" height="26"/>
  <text x="68" y="300" font-family="system-ui,sans-serif" font-size="11" font-weight="600" fill="#007090">Open-Meteo API</text>
  <text x="68" y="313" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">REST · JSON</text>
  <text x="36" y="340" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">555 weather records</text>
</g>

<!-- ══════════════════════════════════════════════════════
     BRONZE LAYER CARDS
══════════════════════════════════════════════════════ -->
<g class="card">
  <rect x="208" y="96" width="122" height="50" rx="8" fill="var(--surface-1,#fff)" stroke="#CD7F32" stroke-width="1.5"/>
  <use href="#ico-adb" x="216" y="104" width="22" height="22"/>
  <text x="244" y="113" font-family="system-ui,sans-serif" font-size="10" font-weight="600" fill="#8B5500">bronze_usage</text>
  <text x="244" y="126" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">Raw parquet ingest</text>
</g>

<g class="card">
  <rect x="208" y="190" width="122" height="50" rx="8" fill="var(--surface-1,#fff)" stroke="#CD7F32" stroke-width="1.5"/>
  <use href="#ico-adb" x="216" y="198" width="22" height="22"/>
  <text x="244" y="207" font-family="system-ui,sans-serif" font-size="10" font-weight="600" fill="#8B5500">bronze_customers</text>
  <text x="244" y="220" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">Raw profiles ingest</text>
</g>

<g class="card">
  <rect x="208" y="284" width="122" height="50" rx="8" fill="var(--surface-1,#fff)" stroke="#CD7F32" stroke-width="1.5"/>
  <use href="#ico-adb" x="216" y="292" width="22" height="22"/>
  <text x="244" y="301" font-family="system-ui,sans-serif" font-size="10" font-weight="600" fill="#8B5500">bronze_weather</text>
  <text x="244" y="314" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">Raw API ingest</text>
</g>

<!-- ══════════════════════════════════════════════════════
     SILVER LAYER CARDS
══════════════════════════════════════════════════════ -->
<g class="card">
  <rect x="373" y="96" width="124" height="62" rx="8" fill="var(--surface-1,#fff)" stroke="#888" stroke-width="1.5"/>
  <use href="#ico-adb" x="381" y="104" width="22" height="22"/>
  <text x="409" y="113" font-family="system-ui,sans-serif" font-size="10" font-weight="600" fill="#444">silver_usage</text>
  <text x="381" y="130" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">DQ · dedup · aggregate</text>
  <text x="381" y="143" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">30-day summaries</text>
</g>

<g class="card">
  <rect x="373" y="200" width="124" height="62" rx="8" fill="var(--surface-1,#fff)" stroke="#888" stroke-width="1.5"/>
  <use href="#ico-adb" x="381" y="208" width="22" height="22"/>
  <text x="409" y="217" font-family="system-ui,sans-serif" font-size="10" font-weight="600" fill="#444">silver_customers</text>
  <text x="381" y="234" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">8 DQ fixes · standardize</text>
  <text x="381" y="247" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">city · date · phone · gender</text>
</g>

<!-- ══════════════════════════════════════════════════════
     GOLD LAYER CARD
══════════════════════════════════════════════════════ -->
<g class="card">
  <rect x="538" y="130" width="126" height="150" rx="8" fill="var(--surface-1,#fff)" stroke="#DAA520" stroke-width="1.5"/>
  <use href="#ico-adb" x="546" y="138" width="22" height="22"/>
  <text x="574" y="148" font-family="system-ui,sans-serif" font-size="10" font-weight="600" fill="#8B6914">gold_churn</text>
  <text x="546" y="168" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">6-signal scoring</text>

  <!-- signal pills -->
  <rect x="546" y="176" width="108" height="14" rx="4" fill="#DAA52018"/>
  <text x="600" y="186" text-anchor="middle" font-family="system-ui,sans-serif" font-size="8" fill="#8B6914">usage drop · complaints</text>
  <rect x="546" y="193" width="108" height="14" rx="4" fill="#DAA52018"/>
  <text x="600" y="203" text-anchor="middle" font-family="system-ui,sans-serif" font-size="8" fill="#8B6914">payment · downgrade</text>
  <rect x="546" y="210" width="108" height="14" rx="4" fill="#DAA52018"/>
  <text x="600" y="220" text-anchor="middle" font-family="system-ui,sans-serif" font-size="8" fill="#8B6914">inactivity · low activity</text>

  <line x1="546" y1="230" x2="654" y2="230" stroke="#DAA520" stroke-width=".5" opacity=".4"/>
  <text x="600" y="243" text-anchor="middle" font-family="system-ui,sans-serif" font-size="9" font-weight="600" fill="#8B6914">Churn Risk</text>

  <!-- risk badges -->
  <rect x="548" y="249" width="32" height="14" rx="4" fill="#FF3621" opacity=".8"/>
  <text x="564" y="259" text-anchor="middle" font-family="system-ui,sans-serif" font-size="8" fill="#fff" font-weight="600">HIGH</text>
  <rect x="585" y="249" width="38" height="14" rx="4" fill="#FF8C00" opacity=".8"/>
  <text x="604" y="259" text-anchor="middle" font-family="system-ui,sans-serif" font-size="8" fill="#fff" font-weight="600">MEDIUM</text>
  <rect x="628" y="249" width="26" height="14" rx="4" fill="#228B22" opacity=".8"/>
  <text x="641" y="259" text-anchor="middle" font-family="system-ui,sans-serif" font-size="8" fill="#fff" font-weight="600">LOW</text>

  <!-- weather intelligence note -->
  <text x="600" y="276" text-anchor="middle" font-family="system-ui,sans-serif" font-size="8" fill="var(--text-secondary,#888)">⛈ weather intelligence</text>
</g>

<!-- ══════════════════════════════════════════════════════
     STAR SCHEMA CARD
══════════════════════════════════════════════════════ -->
<g class="card">
  <rect x="703" y="88" width="230" height="110" rx="10" fill="var(--surface-1,#fff)" stroke="#7B61FF" stroke-width="1.5"/>
  <use href="#ico-star" x="712" y="96" width="26" height="26"/>
  <text x="745" y="106" font-family="system-ui,sans-serif" font-size="11" font-weight="600" fill="#5040BB">Star Schema</text>
  <text x="745" y="120" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">fact_churn + 4 dimensions</text>

  <!-- dim pills -->
  <rect x="712" y="128" width="54" height="14" rx="4" fill="#7B61FF18"/>
  <text x="739" y="138" text-anchor="middle" font-family="system-ui,sans-serif" font-size="8" fill="#5040BB">dim_customer</text>
  <rect x="771" y="128" width="44" height="14" rx="4" fill="#7B61FF18"/>
  <text x="793" y="138" text-anchor="middle" font-family="system-ui,sans-serif" font-size="8" fill="#5040BB">dim_plan</text>
  <rect x="820" y="128" width="44" height="14" rx="4" fill="#7B61FF18"/>
  <text x="842" y="138" text-anchor="middle" font-family="system-ui,sans-serif" font-size="8" fill="#5040BB">dim_usage</text>

  <rect x="712" y="146" width="52" height="14" rx="4" fill="#7B61FF18"/>
  <text x="738" y="156" text-anchor="middle" font-family="system-ui,sans-serif" font-size="8" fill="#5040BB">dim_weather</text>
  <rect x="769" y="146" width="52" height="14" rx="4" fill="#FF362118"/>
  <text x="795" y="156" text-anchor="middle" font-family="system-ui,sans-serif" font-size="8" fill="#CC2010">fact_churn</text>

  <text x="712" y="185" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">SCD Type 1 · Surrogate keys</text>
</g>

<!-- Delta Lake Target -->
<g class="card">
  <rect x="703" y="216" width="105" height="60" rx="10" fill="var(--surface-1,#fff)" stroke="#0078D4" stroke-width="1.5"/>
  <use href="#ico-delta" x="711" y="224" width="22" height="22"/>
  <text x="739" y="233" font-family="system-ui,sans-serif" font-size="10" font-weight="600" fill="#0050A0">Delta Lake</text>
  <text x="711" y="250" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">Analytics team</text>
  <text x="711" y="263" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">10 Delta tables</text>
</g>

<!-- Neon PostgreSQL Target -->
<g class="card">
  <rect x="820" y="216" width="113" height="60" rx="10" fill="var(--surface-1,#fff)" stroke="#336791" stroke-width="1.5"/>
  <use href="#ico-pg" x="828" y="224" width="22" height="22"/>
  <text x="856" y="233" font-family="system-ui,sans-serif" font-size="10" font-weight="600" fill="#1a4a6b">Neon PostgreSQL</text>
  <text x="828" y="250" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">Business team</text>
  <text x="828" y="263" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">6 tables via JDBC</text>
</g>

<!-- Power BI -->
<g class="card">
  <rect x="703" y="294" width="230" height="56" rx="10" fill="var(--surface-1,#fff)" stroke="#F2C811" stroke-width="1.5"/>
  <use href="#ico-pbi" x="712" y="302" width="26" height="26"/>
  <text x="746" y="314" font-family="system-ui,sans-serif" font-size="11" font-weight="600" fill="#9B7A00">Power BI Dashboard</text>
  <text x="746" y="328" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">5 pages · Churn KPIs · City heatmap · Retention actions</text>
</g>

<!-- ══════════════════════════════════════════════════════
     ANIMATED FLOW LINES — Source to Bronze
══════════════════════════════════════════════════════ -->
<!-- ADLS → bronze_usage -->
<line x1="158" y1="122" x2="208" y2="122" stroke="#0078D4" stroke-width="2" fill="none" marker-end="url(#arr)" class="flow-h"/>
<!-- PostgreSQL → bronze_customers -->
<line x1="158" y1="216" x2="208" y2="216" stroke="#336791" stroke-width="2" fill="none" marker-end="url(#arr)" class="flow-h"/>
<!-- WebAPI → bronze_weather -->
<line x1="158" y1="310" x2="208" y2="310" stroke="#00B4D8" stroke-width="2" fill="none" marker-end="url(#arr)" class="flow-h"/>

<!-- Bronze to Silver -->
<!-- bronze_usage → silver_usage -->
<line x1="330" y1="122" x2="373" y2="122" stroke="#CD7F32" stroke-width="2" fill="none" marker-end="url(#arr)" class="flow-h"/>
<!-- bronze_customers → silver_customers -->
<line x1="330" y1="216" x2="373" y2="230" stroke="#CD7F32" stroke-width="2" fill="none" marker-end="url(#arr)" class="flow-h"/>
<!-- bronze_weather skips silver → goes to gold -->
<path d="M330 310 L358 310 L358 205 L538 205" fill="none" stroke="#CD7F32" stroke-width="1.5" marker-end="url(#arr)" class="flow-h" stroke-dasharray="6 4"/>

<!-- Silver to Gold -->
<line x1="497" y1="128" x2="538" y2="180" stroke="#888" stroke-width="2" fill="none" marker-end="url(#arr)" class="flow-h"/>
<line x1="497" y1="231" x2="538" y2="220" stroke="#888" stroke-width="2" fill="none" marker-end="url(#arr)" class="flow-h"/>

<!-- Gold to Star Schema -->
<line x1="664" y1="205" x2="703" y2="145" stroke="#DAA520" stroke-width="2" fill="none" marker-end="url(#arr)" class="flow-h"/>

<!-- Star Schema to Targets -->
<line x1="755" y1="198" x2="755" y2="216" stroke="#7B61FF" stroke-width="1.5" fill="none" marker-end="url(#arr)" class="flow-v"/>
<line x1="876" y1="198" x2="876" y2="216" stroke="#7B61FF" stroke-width="1.5" fill="none" marker-end="url(#arr)" class="flow-v"/>

<!-- Delta Lake to Power BI -->
<line x1="755" y1="276" x2="755" y2="294" stroke="#0078D4" stroke-width="1.5" fill="none" marker-end="url(#arr)" class="flow-v"/>

<!-- ══════════════════════════════════════════════════════
     LEGEND
══════════════════════════════════════════════════════ -->
<rect x="20" y="528" width="920" height="60" rx="10" fill="var(--surface-1,#fff)" stroke="var(--border,#e0e0e0)" stroke-width="1"/>
<text x="40" y="548" font-family="system-ui,sans-serif" font-size="10" font-weight="600" fill="var(--text-secondary,#888)">LEGEND</text>
<line x1="40" y1="562" x2="65" y2="562" stroke="#0078D4" stroke-width="2" stroke-dasharray="6 4"/>
<text x="70" y="566" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">Data flow</text>
<rect x="145" y="556" width="10" height="10" rx="2" fill="none" stroke="#CD7F32" stroke-width="1.5"/>
<text x="160" y="566" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">Bronze layer</text>
<rect x="240" y="556" width="10" height="10" rx="2" fill="none" stroke="#888" stroke-width="1.5"/>
<text x="255" y="566" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">Silver layer</text>
<rect x="330" y="556" width="10" height="10" rx="2" fill="none" stroke="#DAA520" stroke-width="1.5"/>
<text x="345" y="566" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">Gold layer</text>
<rect x="415" y="556" width="10" height="10" rx="2" fill="none" stroke="#FFB900" stroke-width="1.5"/>
<text x="430" y="566" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">Key Vault secured</text>
<rect x="530" y="556" width="10" height="10" rx="2" fill="none" stroke="#7B61FF" stroke-width="1.5"/>
<text x="545" y="566" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">Star schema model</text>
<rect x="645" y="556" width="10" height="10" rx="2" fill="none" stroke="#F2C811" stroke-width="1.5"/>
<text x="660" y="566" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">Power BI dashboard</text>
<text x="760" y="566" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#888)">Dual write → Delta Lake + PostgreSQL</text>

<!-- record counts at bottom -->
<text x="480" y="604" text-anchor="middle" font-family="system-ui,sans-serif" font-size="10" fill="var(--text-secondary,#888)">
  91,799 usage records · 5,000 customer profiles · 555 weather records → 5,000 customers scored · HIGH / MEDIUM / LOW churn risk
</text>

<!-- Databricks branding note -->
<text x="480" y="624" text-anchor="middle" font-family="system-ui,sans-serif" font-size="9" fill="var(--text-secondary,#666)" opacity=".7">
  All transformations run on Azure Databricks · 7 PySpark notebooks · Orchestrated by Azure Data Factory · Daily scheduled trigger
</text>

</svg>
# 🏗️ Architecture

[telecom_churn_azure_pipeline_architecture.html](https://github.com/user-attachments/files/30463007/telecom_churn_azure_pipeline_architecture.html)

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
<img width="1915" height="1078" alt="Screenshot 2026-07-27 200530" src="https://github.com/user-attachments/assets/039d2b43-11e3-4ea0-a35f-5bfc6b51fefe" />


### Databricks Tables
[<img width="474" height="428" alt="image" src="https://github.com/user-attachments/assets/91e3fc10-59b5-48ad-b194-6023a2035261" />
]

### Power BI Dashboard
[INSERT POWER BI SCREENSHOT]

---

## 👨‍💻 Author

**Yogin N C**
Azure Data Engineer
📧 yoginnc@gmail.com
🔗 https://www.linkedin.com/in/yogin-nc-2b006324b/
🐙 https://github.com/Yoginsjce?tab=repositories
