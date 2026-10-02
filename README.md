# WideWorldImporters — Microsoft Fabric Data Engineering Project

An end-to-end data engineering project that brings WideWorldImporters data from Azure SQL into Microsoft Fabric, processes it through Bronze, Silver and Gold layers, and presents sales insights in Power BI.

## Architecture

Azure SQL → Bronze Lakehouse → Silver Lakehouse → Gold Lakehouse → Semantic Model → Power BI Report

## Technology Stack

- **Azure SQL Database:** Source data and business-day simulation
- **Microsoft Fabric Data Factory:** Data ingestion and pipeline orchestration
- **Fabric Notebooks:** PySpark and Spark SQL transformations
- **OneLake and Delta Lake:** Data storage and curated tables
- **Power BI:** Semantic model, measures and reporting
- **GitHub:** Version control for Fabric item definitions

## Data Layers

| Layer | Purpose |
|---|---|
| Bronze — `wwi_bronze` | Stores source extracts as Parquet files, organized by table, ingestion date, batch and attempt |
| Silver — `wwi_silver` | Cleans and standardizes data, applies incremental updates and validates table quality |
| Gold — `wwi_gold` | Builds business-ready dimensions and an invoice-line fact table for sales reporting |

The ingestion framework covers **48 source tables** across the Application, Purchasing, Sales and Warehouse schemas.

## Main Workflows

### Full Load
Loads the initial source data into Bronze, transforms it into Silver and builds the Gold reporting tables.

### Incremental Load
Uses table-specific extraction methods and watermarks to process new or changed data. Snapshot extraction is used for selected tables.

### Data Simulation
Azure SQL stored procedures generate additional WWI business days to test incremental processing.

### Validation
Checks source and destination row counts, required keys, duplicates and batch completeness. Success manifests record approved processing results.

## Gold Reporting Model

| Table | Purpose |
|---|---|
| `common.dim_date` | Calendar attributes for time-based analysis |
| `sales.dim_customer` | Customer and geographic attributes |
| `inventory.dim_stock_item` | Product attributes |
| `sales.fact_invoice_line` | Invoice-line sales, quantities, tax and profit |

## Power BI Report

The report includes:

- Total Sales
- Sales Excluding Tax
- Total Profit
- Profit Margin %
- Quantity Sold
- Invoice Count
- Monthly sales trends
- Sales by territory and state/province

## Operational Tracking

Configuration tables, audit logs and manifests support:

- Table-specific ingestion settings
- Watermark tracking
- Batch and attempt identification
- Validation results
- Traceability between Bronze, Silver and Gold runs

## Project Organization

- `01_data` — Lakehouse definitions
- `02_pipelines` — Ingestion and orchestration pipelines
- `03_notebooks` — Transformation, validation and setup notebooks
- `04_reporting_models` — Reporting assets
- Semantic model and report definitions

## Running the Project

1. Configure the Azure SQL connection and Fabric lakehouses.
2. Initialize operational tables and source metadata.
3. Run the full-load workflow to establish the baseline.
4. Run the incremental workflow for subsequent business days.
5. Review validation logs and success manifests.
6. Verify the latest results in the Power BI report.

Pipeline parameters carry batch IDs, attempt IDs and processing boundaries between activities.

## Repository Scope

This repository contains Fabric item definitions and project code. Source data, OneLake data files and connection credentials are not included.
