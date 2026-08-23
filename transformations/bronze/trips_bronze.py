from pyspark import pipelines as dp
import pyspark.sql.functions as F

SOURCE_PATH = "s3://goodcabs-data-daya1an/datastore/trips"

# Define a streaming ingestion table using Lakeflow Declarative Pipelines Auto Loader (STREAMING TABLE)
@dp.table(
    name="transportation.bronze.trips",  # Fully qualified table name in Unity Catalog
    comment="Streaming ingestion of raw orders data with Auto Loader",  # Description for documentation and metadata
    table_properties={
        # Custom table properties for quality, layer, and ingestion optimization
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",   # Enables change data feed for downstream CDC use
        "delta.autoOptimize.optimizeWrite": "true",  # Write optimization for Delta Lake
        "delta.autoOptimize.autoCompact": "true",    # Auto compaction to optimize storage
    },
)
def orders_bronze():
    # Read streaming CSV files from cloud storage using Auto Loader
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")                   # Specify CSV file format
        .option("cloudFiles.inferColumnTypes", "true")        # Infer column types automatically
        .option("cloudFiles.schemaEvolutionMode", "rescue")   # Handle schema drift by rescuing unexpected columns
        .option("cloudFiles.maxFilesPerTrigger", 100)         # Process up to 100 new files per trigger
        .load(SOURCE_PATH)                                    # Cloud path to streaming raw source data
    )

    # Rename column to ensure compatibility with downstream processing
    df = df.withColumnRenamed(
        "distance_travelled(km)",
        "distance_travelled_km"
    )

    # Add metadata columns: source file name and ingestion timestamp for traceability
    df = df.withColumn("file_name", F.col("_metadata.file_path")) \
           .withColumn("ingest_datetime", F.current_timestamp())

    # Return transformed DataFrame to be written as a Delta/streaming table
    return df
