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

You can make it neutral and solution-focused instead of using "your":

## SCD Type 1 vs. SCD Type 2

**SCD Type 1** simply means **"overwrite the old value with the new one."** If a trip's fare gets corrected from $12 to $15, the $12 disappears and only $15 remains. No history is kept of the change.

**SCD Type 2** means **"keep every version with start/end dates."** The $12 fare stays in the table marked as the old version, and the $15 fare is added as a new version, allowing the fare value to be traced at any point in time.

### Why Type 1 is the right choice in this scenario

1. **Trips are events, not profiles.**  
   SCD Type 2 is best suited for attributes that change slowly over time, such as customer addresses or product categories. Trip records are transactional events where the objective is to retain the final corrected value.

2. **Analytics require the current source of truth.**  
   When reporting on a trip, consumers typically expect a single authoritative fare value rather than multiple historical versions of the same trip record.

3. **No reliable change timestamp is available.**  
   SCD Type 2 depends on a reliable mechanism to determine when a change occurred so that versions can be sequenced accurately. The source dataset does not provide such information.

4. **The Bronze layer retains raw history.**  
   Since the Bronze layer preserves the original ingested records, historical values remain available for audit, validation, or replay purposes without introducing Type 2 complexity in the Silver layer.

### Summary

**Silver stores the latest corrected value using SCD Type 1, while Bronze preserves the raw historical records.** This approach keeps the Silver layer simple, performant, and aligned with reporting requirements.

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
export_path = "s3://my-s3-bucket/exports/fact_trips_csv"

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
