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

<img width="1536" height="772" alt="image" src="https://github.com/user-attachments/assets/25bcc446-1bd5-42a0-9fe4-636d105f8ecd" />

## Why SCD Type 1 ?

For this project, SCD Type 1 is the better choice if the Silver trips table is meant to show the latest corrected trip state for dashboards and operational analytics.
Why:
- Trips are generally fact records, not slowly changing dimensions.
- Your Gold views need the current fare, rating, city, and distance, not multiple historical versions of one trip.
- Your source lacks a business update timestamp or version field, which is important for trustworthy Type 2 sequencing.
- Bronze already preserves raw ingested data, providing a basic audit and replay layer.

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

## Export the views to CSV
_(currently not present)_

```python
export_path = "s3://goodcabs-data-daya1an/exports/fact_trips_csv"

(
    spark.table("transportation.gold.fact_trips")
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("delimiter", ",")
    .csv(export_path)
)
```

## Business value

The lakehouse converts raw ride-hailing events into governed, query-ready data. This reduces repetitive data preparation and gives city teams a consistent foundation for operational analysis, trip trends, and service-quality reporting.
