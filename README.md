# 🚚 Supply Chain & Logistics Analytics

### End-to-End Data Engineering and Business Intelligence Project using Databricks, Apache Spark, Delta Lake and Power BI

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-003366?style=for-the-badge)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)

---

## 📌 1. Project Overview

Supply Chain & Logistics Analytics is an end-to-end data engineering and business intelligence project designed to analyse supply chain operations, delivery performance, regional sales and shipping efficiency.

The project uses the **DataCo Supply Chain Dataset**, containing approximately 180,519 records, to build a structured data processing pipeline using Apache Spark and Databricks.

The data is processed through the Medallion Architecture, consisting of Bronze, Silver and Gold layers. The transformed data is then used to generate business analytics and interactive visualizations in Microsoft Power BI.

The primary objective is to transform raw supply chain data into meaningful business insights that can help organizations understand operational performance and identify areas for improvement.

## 🎯 2. Problem Statement

Supply chain organizations generate large volumes of operational data related to customer orders, product sales, shipments, delivery performance and regional distribution.

However, analysing this data efficiently is challenging because of data quality issues, fragmented information and the difficulty of identifying operational bottlenecks.

This project addresses these challenges by developing an end-to-end data engineering pipeline that:

- Processes large-scale supply chain datasets.
- Cleans and transforms raw operational data.
- Organizes data using Medallion Architecture.
- Generates analytical datasets for business reporting.
- Evaluates delivery performance and shipping efficiency.
- Provides interactive business intelligence dashboards.

## 🎯 3. Project Objectives

1. Build an end-to-end data engineering pipeline using Databricks.
2. Implement Bronze, Silver and Gold data layers.
3. Perform data cleaning and transformation using PySpark.
4. Store and process data using Delta Lake.
5. Generate business-oriented analytical datasets.
6. Analyse sales, profit, delivery and shipping performance.
7. Integrate Databricks with Microsoft Power BI.
8. Develop an interactive dashboard for business analysis.

## 📊 4. Dataset Information

**Dataset:** DataCo Supply Chain Dataset

**Format:** CSV

**Total Records:** 180,519

**Raw Storage:** Databricks Volume

**Storage Location:**
`workspace.default.logistics_raw`

The dataset contains supply chain information covering areas such as:

- Customer orders
- Product information
- Sales and profitability
- Shipping modes
- Delivery status
- Delivery risk
- Regional and country information
- Order and shipping dates

The dataset is used to analyse supply chain operations and generate business performance indicators.

## 🏗️ 5. System Architecture

The project follows the Medallion Architecture implemented in Databricks.

### Architecture Flow

```text
             DATACO SUPPLY CHAIN DATASET
                    CSV FILE
                       |
                       ▼
                DATA INGESTION
                 Apache Spark
                       |
                       ▼
                RAW DATA STORAGE
                Databricks Volume
                       |
                       ▼
              ┌───────────────────┐
              │   BRONZE LAYER   │
              │   Raw Delta Data │
              └───────────────────┘
                       |
                       ▼
              ┌───────────────────┐
              │   SILVER LAYER   │
              │ Cleaning &       │
              │ Transformation  │
              └───────────────────┘
                       |
                       ▼
              ┌───────────────────┐
              │    GOLD LAYER    │
              │ Business         │
              │ Aggregations     │
              └───────────────────┘
                       |
                       ▼
              ┌───────────────────┐
              │  DATABRICKS SQL  │
              │    WAREHOUSE     │
              └───────────────────┘
                       |
                       ▼
              ┌───────────────────┐
              │   POWER BI       │
              │   DirectQuery    │
              └───────────────────┘
                       |
                       ▼
             INTERACTIVE DASHBOARD
```

### Medallion Architecture

| Layer | Purpose | Implementation |
|---|---|---|
| Bronze | Stores ingested raw data | Raw Delta Table |
| Silver | Cleans and transforms data | Cleaned Delta Table |
| Gold | Generates business analytics | Aggregated Gold Tables |

## ⚙️ 6. Data Engineering Pipeline

The ETL pipeline is implemented using Databricks notebooks and Databricks Workflows.

### Step 1: Data Ingestion — Bronze Layer

The raw CSV dataset is loaded from Databricks Volume storage using Apache Spark.

**Notebook:** `01_Bronze_Ingestion`

**Bronze Table:**
`workspace.bronze.dataco_supply_chain_raw`

Key operations:

- Read raw CSV data.
- Apply initial ingestion logic.
- Store the data in Delta format.
- Maintain the original source records.

**Bronze Layer Statistics:**

- Records: 180,519
- Columns: 53

### Step 2: Data Cleaning — Silver Layer

The Bronze data is processed to improve data quality and consistency.

**Notebook:** `02_Silver_Transformation`

**Silver Table:**
`workspace.silver.dataco_supply_chain_clean`

Key operations:

- Remove unnecessary PII columns.
- Convert date fields into appropriate date formats.
- Cast numeric columns into appropriate data types.
- Prepare structured data for analytical processing.
- Validate important data quality conditions.

**Silver Layer Statistics:**

- Records: 180,519
- Columns: 46

### Step 3: Business Analytics — Gold Layer

The Silver data is aggregated into business-oriented analytical tables.

**Notebook:** `03_Gold_Analytics_New`

The Gold layer contains four analytical tables:

| Gold Table | Purpose |
|---|---|
| `gold_logistics_kpis` | Overall logistics KPIs |
| `gold_delivery_performance` | Delivery status and performance analysis |
| `gold_regional_analysis` | Country and regional performance |
| `gold_shipping_analysis` | Shipping mode and profitability analysis |

These tables provide the analytical foundation for the Power BI dashboard.

## 🔄 7. Workflow Orchestration

The project uses Databricks Workflows to organize and execute the ETL pipeline.

**Workflow Name:** `Logistics_Supply_Chain_ETL`

Pipeline execution:

```text
Bronze Ingestion
       |
       ▼
Silver Transformation
       |
       ▼
Gold Analytics
```

The workflow coordinates the execution of the data processing notebooks in the required sequence.

## 📈 8. Data Analysis and Business KPIs

The project generates multiple business performance indicators.

### Overall Business Metrics

| Metric | Result |
|---|---:|
| Total Sales | $36,784,735.01 |
| Total Profit | $3,966,902.97 |
| Total Order Items | 180,519 |
| Unique Orders | 65,752 |
| Cancelled Items | 7,754 |
| Late Shipment Items | 98,977 |
| Late Delivery Percentage | 57.29% |

*Note: The late delivery percentage uses non-cancelled order items as its denominator.*

### Key Analytical Areas

**1. Sales and Profit Analysis**
- Overall sales performance.
- Profitability analysis.
- Regional sales contribution.

**2. Delivery Performance**
- Delivery status distribution.
- Late delivery analysis.
- Actual versus scheduled shipping days.

**3. Regional Analysis**
- Country-wise sales performance.
- Regional delivery performance.
- Late shipment comparison.

**4. Shipping Analysis**
- Shipping mode distribution.
- Shipping mode performance.
- Profit comparison across shipping modes.

## 📊 9. Power BI Dashboard

Microsoft Power BI is used to create the final interactive business intelligence dashboard.

The dashboard connects to Databricks SQL Warehouse using DirectQuery.

### Dashboard Sections

**1. Executive Overview**
- Total Sales
- Total Profit
- Unique Orders
- Total Order Items
- Cancelled Shipments
- Late Shipments
- Late Delivery Percentage

**2. Delivery Performance**
- Delivery Status Distribution
- Actual vs Scheduled Shipping Days
- Shipment Count by Delivery Status

**3. Regional Performance**
- Top 10 Countries by Sales
- Late Delivery Percentage by Region
- Regional Shipment Performance

**4. Shipping Analysis**
- Shipments by Shipping Mode
- Late Delivery Percentage by Shipping Mode
- Profit by Shipping Mode

### Dashboard Features

- Interactive business intelligence visualizations.
- Market-level filtering.
- Regional filtering.
- Country-level filtering.
- Shipping mode filtering.
- Dynamic KPI analysis.

## 🛠️ 10. Technology Stack

| Technology | Usage |
|---|---|
| Python | Programming language |
| Apache Spark | Distributed data processing |
| PySpark | Data transformation |
| Databricks | Data engineering platform |
| Delta Lake | Structured data storage |
| Databricks Volumes | Raw data storage |
| Databricks Workflows | ETL orchestration |
| Databricks SQL | Analytical data access |
| Microsoft Power BI | Interactive dashboards |
| Git | Version control |
| GitHub | Project hosting |

## 📁 11. Project Structure

The following structure represents the recommended GitHub organization for this project. Adjust the paths to match the actual files in your workspace.

```text
Supply-Chain-Logistics-Analytics/
│
├── notebooks/
│   ├── 01_Bronze_Ingestion
│   ├── 02_Silver_Transformation
│   └── 03_Gold_Analytics_New
│
├── data/
│   └── README.md
│
├── dashboard/
│   ├── Supply_Chain_Logistics_Analytics.pbix
│   └── Supply_Chain_Architecture.png
│
├── documentation/
│   ├── architecture.md
│   ├── Supply_Chain_Architecture.drawio
│   └── Project_Report.pdf
│
├── README.md
│
└── .gitignore
```

## 🚀 12. Implementation and Execution

### Prerequisites

- Databricks workspace.
- Apache Spark / PySpark.
- Access to Databricks Volumes.
- Databricks SQL Warehouse.
- Microsoft Power BI Desktop.
- Git and GitHub account.

### Implementation Steps

1. Upload the DataCo Supply Chain CSV dataset to Databricks Volume storage.
2. Execute the Bronze ingestion notebook.
3. Execute the Silver transformation notebook.
4. Execute the Gold analytics notebook.
5. Configure and run the Databricks ETL workflow.
6. Verify the Gold analytical tables.
7. Connect Power BI to Databricks SQL Warehouse.
8. Import the Gold tables using DirectQuery.
9. Develop and save the Power BI dashboard.

*Note: The exact notebook paths and connection settings depend on your Databricks workspace configuration.*

## 🔍 13. Data Quality and Validation

The project includes data validation checks performed during development and auditing.

Key validation results:

- Bronze and Silver row counts reconciled.
- Unique `order_item_id` values verified.
- No duplicate order-item records found during the audit.
- Sales and profit fields checked for null values.
- Shipping dates checked against order dates.
- Delivery risk consistency checked against delivery status.
- Gold analytical totals reconciled with Silver data.

These checks help improve the reliability of the analytical outputs.

## 💡 14. Business Value

This project demonstrates how data engineering and business intelligence can support supply chain decision-making.

Potential business benefits include:

- Better visibility into delivery performance.
- Identification of regions with higher late-delivery rates.
- Improved understanding of shipping mode performance.
- Analysis of sales and profitability.
- Identification of operational bottlenecks.
- Data-driven supply chain planning.

## 🔮 15. Future Enhancements

Possible future improvements include:

- Automated data ingestion from external APIs.
- Scheduled ETL pipeline execution.
- Advanced delivery delay prediction using Machine Learning.
- Demand forecasting.
- Inventory optimization.
- Real-time shipment monitoring.
- Automated data quality alerts.
- Enhanced business intelligence reporting.

## 👨‍💻 16. Author

**Project:** Supply Chain & Logistics Analytics

**Domain:** Data Engineering and Business Intelligence

**Technologies:** Databricks | Apache Spark | PySpark | Delta Lake | Power BI

---

## 📄 17. License

This project is developed for educational and academic purposes.

Dataset usage and redistribution are subject to the original dataset's licensing and usage conditions.
