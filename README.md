# Supply Chain & Logistics Analytics

### End-to-End Data Engineering and Business Intelligence using Databricks, Apache Spark, Delta Lake and Power BI

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)

## 1. Project Overview

Supply Chain & Logistics Analytics is an end-to-end data engineering and business intelligence project. It processes the DataCo Supply Chain Dataset using Databricks, Apache Spark, PySpark and Delta Lake, then presents business-oriented insights through Microsoft Power BI.

The pipeline follows the Medallion Architecture: Bronze for raw ingestion, Silver for cleaning and transformation, and Gold for analytical datasets.

## 2. Problem Statement

Supply chain operations generate data about orders, products, sales, shipments, delivery status and geography. Raw operational data is difficult to use directly for consistent business analysis. This project builds a repeatable data pipeline to prepare the data and produce insights into sales, profitability, delivery performance, regional trends and shipping modes.

## 3. Objectives

- Ingest and store the raw supply chain dataset in Databricks.
- Implement Bronze, Silver and Gold data layers.
- Clean and transform records using PySpark.
- Generate reusable analytical Gold tables.
- Orchestrate processing through Databricks Workflows.
- Connect Databricks SQL to Power BI using DirectQuery.
- Build an interactive supply chain analytics dashboard.

## 4. Dataset

- **Dataset:** DataCo Supply Chain Dataset
- **Format:** CSV
- **Reported records:** 180,519
- **Raw storage:** `workspace.default.logistics_raw`
- **Bronze:** 180,519 rows and 53 columns
- **Silver:** 180,519 rows and 46 columns

The dataset includes order, sales, profit, delivery, shipping mode and geographic information. Add the verified original dataset URL and license details before public distribution.

## 5. Architecture

The solution uses the following logical flow:

```text
DataCo Supply Chain CSV
          |
          v
Databricks Volume (raw storage)
          |
          v
Bronze: dataco_supply_chain_raw
          |
          v
Silver: dataco_supply_chain_clean
          |
          v
Gold analytical tables
  |          |          |          |
  v          v          v          v
Logistics  Delivery   Regional   Shipping
  KPIs    Performance Analysis   Analysis
          |
          v
Databricks SQL Warehouse
          |
          v
Power BI (DirectQuery)
          |
          v
Interactive Analytics Dashboard
```

See `documentation/Supply_Chain_Architecture.png` for the visual architecture and `documentation/Supply_Chain_Architecture.drawio` for the editable diagram.

## 6. Medallion Architecture and Notebooks

### Bronze Layer
- Notebook: `01_Bronze_Ingestion`
- Table: `workspace.bronze.dataco_supply_chain_raw`
- Purpose: ingest and store the source records in Delta format.
- Audited size: 180,519 rows, 53 columns.

### Silver Layer
- Notebook: `02_Silver_Transformation`
- Table: `workspace.silver.dataco_supply_chain_clean`
- Purpose: remove unnecessary PII columns, parse date fields and cast numeric fields.
- Audited size: 180,519 rows, 46 columns.

### Gold Layer
- Notebook: `03_Gold_Analytics_New`
- Schema: `workspace.gold`
- Purpose: create business-focused aggregated datasets.

| Gold table | Purpose |
|---|---|
| `gold_logistics_kpis` | Overall logistics and business KPIs |
| `gold_delivery_performance` | Delivery status and shipping-day analysis |
| `gold_regional_analysis` | Market, region and country analysis |
| `gold_shipping_analysis` | Shipping mode and profitability analysis |

## 7. Workflow Orchestration

**Workflow:** `Logistics_Supply_Chain_ETL`

Expected execution sequence:

1. Bronze ingestion
2. Silver transformation
3. Gold analytics

The active workflow configuration identified during the project audit uses `03_Gold_Analytics_New`. A duplicate legacy job was also identified; review it before archiving or deleting it.

## 8. Key Business Metrics

Reported aggregate results from the project audit:

| Metric | Result |
|---|---:|
| Total sales | $36,784,735.01 |
| Total profit | $3,966,902.97 |
| Order-item records | 180,519 |
| Unique orders | 65,752 |
| Cancelled order items | 7,754 |
| Non-cancelled order items | 172,765 |
| Late order items | 98,977 |
| Late delivery percentage (KPI) | 57.29% |

The 57.29% KPI is calculated as late order items divided by non-cancelled order items. Confirm denominator consistency across all Gold tables before comparing regional and shipping percentages.

**Metric definition note:** Some aggregate fields named `total_shipments` count order-item rows rather than unique shipments or orders. Interpret these as order-item counts unless the definition is changed and validated.

## 9. Power BI Dashboard

Power BI connects to Databricks SQL Warehouse through DirectQuery and contains four analytical areas:

- **Executive Overview:** total sales, profit, unique orders, order items, cancellations, late items and late-delivery percentage.
- **Delivery Performance:** delivery status distribution, actual versus scheduled shipping days and delivery-status counts.
- **Regional Performance:** top countries by sales, late delivery percentage by region and regional performance table.
- **Shipping Analysis:** shipment/order-item counts by shipping mode, late delivery percentage by mode and profit by mode.

Available report filters include market, order region, order country and shipping mode.

## 10. Data Quality and Validation

The technical audit reported:
- Bronze and Silver row counts reconciled at 180,519.
- All 180,519 `order_item_id` values were unique in the audited data.
- No nulls were reported in the audited sales and profit fields.
- No shipping-before-ordering dates were reported.
- The delivery-risk indicator matched late delivery status in the audited records.
- Gold totals were reconciled against Silver.

Audit follow-ups included standardizing late-delivery percentage denominators, excluding cancelled records from shipping-day averages, adding explicit cast-null validation gates and considering minimum sample thresholds for regional rates. Re-run validations after any notebook changes.

## 11. Technology Stack

| Technology | Usage |
|---|---|
| Python / PySpark | Data processing and transformation |
| Apache Spark | Distributed data processing |
| Databricks | Data engineering workspace |
| Delta Lake | Structured data storage |
| Databricks Volumes | Raw CSV storage |
| Databricks Workflows | ETL orchestration |
| Databricks SQL Warehouse | Analytical data access |
| Microsoft Power BI | Dashboard and reporting |
| Git and GitHub | Version control and project hosting |

## 12. Repository Structure

```text
Supply-Chain-Logistics-Analytics/
├── notebooks/
│   ├── 01_Bronze_Ingestion
│   ├── 02_Silver_Transformation
│   └── 03_Gold_Analytics_New
├── dashboard/
│   └── Supply_Chain_Logistics_Analytics.pbix
├── documentation/
│   ├── Supply_Chain_Architecture.png
│   ├── Supply_Chain_Architecture.drawio
│   ├── Supply_Chain_Logistics_Analytics_Project_Report.pdf
│   └── Supply_Chain_Logistics_Analytics_Presentation.pptx
├── README.md
└── .gitignore
```

This is the intended organization. Update names and paths to match the files actually included in the repository.

## 13. Setup and Execution

1. Upload the dataset to a Databricks Volume.
2. Run `01_Bronze_Ingestion`.
3. Run `02_Silver_Transformation`.
4. Run `03_Gold_Analytics_New`.
5. Verify the four Gold tables in `workspace.gold`.
6. Run the `Logistics_Supply_Chain_ETL` workflow.
7. Connect Power BI Desktop to the Databricks SQL Warehouse.
8. Load the Gold tables using DirectQuery and open the report.

Exact notebook paths, warehouse settings and credentials depend on the Databricks workspace. Never commit credentials, access tokens or `.env` files.

## 14. Business Value

The solution provides a consolidated view of sales, profitability, delivery outcomes, regional performance and shipping modes. These insights can help stakeholders investigate operational bottlenecks and support data-informed supply chain decisions.

## 15. Future Enhancements

- Automated ingestion from a verified external source or API.
- Scheduled ETL runs and workflow notifications.
- Stronger automated data-quality assertions and failure gates.
- Delivery-delay prediction and demand forecasting.
- Inventory optimization and enhanced time-series reporting.

## 16. Author and Academic Context

- **Project:** Supply Chain & Logistics Analytics
- **Programme:** B.Tech – Computer Science & Engineering (AI & ML)
- **Institution:** K.R. Mangalam University
- **Domain:** Data Engineering and Business Intelligence

## 17. References

- [Databricks Documentation](https://docs.databricks.com/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Microsoft Power BI Documentation](https://learn.microsoft.com/power-bi/)

The original DataCo dataset source and license should be cited using the verified publication/download page.
