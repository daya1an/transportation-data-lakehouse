# Transportation Data Lakehouse

A Databricks Medallion lakehouse that transforms ride-hailing data from AWS S3 into reliable, city-level datasets for operational reporting and business analytics across India.

## Project highlights

- Processes **356K+ transportation records** across trip, city, and date datasets
- Runs **incremental batch ingestion** through Auto Loader in trigger-once mode
- Uses **Spark Declarative Pipelines** and Delta Lake for managed, reliable transformations
- Applies data-quality rules and **SCD Type 1** updates for changed trip records
- Produces SQL-ready Gold datasets and city-specific reporting views
- Supports approximately **1.8K daily incremental records**

## Architecture

```text
AWS S3 raw files
      ↓
Bronze Layer ── Raw, append-only source records
      ↓
Silver Layer ── Cleaned, validated, and deduplicated data
      ↓
Gold Layer ──── Analytics-ready dimensional views
      ↓
City-level reporting datasets and Databricks SQL dashboards
```

## Pipeline screenshots

<img width="1031" height="499" alt="image" src="https://github.com/user-attachments/assets/3f0c4d24-eee1-4298-97e5-e89055273dfd" />

## Data flow

1. **Ingest from AWS S3**  
   Auto Loader detects new trip, city, and date files and loads them incrementally in trigger-once mode.

2. **Bronze layer**  
   Raw source data is retained in Delta tables to preserve an auditable, replayable copy of each ingest.

3. **Silver layer**  
   PySpark transformations standardize columns, validate ratings and dates, remove invalid records, and apply SCD Type 1 updates to changed trip data. A calendar dimension with holiday indicators is also created.

4. **Gold layer**  
   SQL views join curated trip, city, and date data into analytics-ready datasets for reporting.

5. **City-level analytics**  
   Gold datasets are organized by city, enabling teams in Jaipur, Kochi, Surat, and other locations to analyze relevant local operations.

## Business value

The lakehouse converts raw ride-hailing events into governed, query-ready data. This reduces repetitive data preparation and gives city teams a consistent foundation for operational analysis, trip trends, and service-quality reporting.
