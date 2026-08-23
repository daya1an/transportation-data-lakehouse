from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import md5, concat_ws, sha2

# ============================================================
# Configuration
# ============================================================

# Source location containing the raw City CSV files.
# Keeping the source path as a configuration variable makes
# it easier to change environments or source locations later.
SOURCE_PATH = "s3://goodcabs-data-daya1an/datastore/city"


# ============================================================
# Bronze Layer: City Data
# ============================================================

# @dp.materialized_view defines this function as a Databricks
# Lakeflow Declarative Pipeline materialized view.
#
# The Bronze layer stores raw data with minimal transformation.
# This allows us to preserve the source data while adding
# ingestion-related metadata for tracking and debugging.
@dp.materialized_view(
    name="transportation.bronze.city",

    # Description shown in the Databricks catalog.
    comment="City Raw Data Processing",

    # Table properties used to describe and optimize the table.
    table_properties={
        "quality": "bronze",                       # Data quality/layer classification
        "layer": "bronze",                         # Identifies this as the Bronze layer
        "source_format": "csv",                   # Original source format

        # Enables Change Data Feed so downstream processes
        # can identify changes made to the Delta table.
        "delta.enableChangeDataFeed": "true",

        # Optimizes file layout during writes.
        "delta.autoOptimize.optimizeWrite": "true",

        # Automatically compacts small files to improve
        # query performance and reduce file overhead.
        "delta.autoOptimize.autoCompact": "true"
    }
)
def city_bronze():

    # Read the raw City data from the configured S3 location.
    # The Bronze layer intentionally performs minimal transformation.
    df = (
        spark.read
            .format("csv")

            # Treat the first row of the CSV as column headers.
            .option("header", "true")

            # Automatically infer appropriate data types
            # instead of reading every column as a string.
            .option("inferSchema", "true")

            # PERMISSIVE mode allows Spark to continue processing
            # even when some records contain malformed data.
            .option("mode", "PERMISSIVE")

            # Allows schema differences to be handled when
            # reading the source data.
            .option("mergeSchema", "true")

            # Stores corrupted records in a separate column
            # instead of failing the entire pipeline.
            .option("columnNameOfCorruptRecord", "_corrupt_record")

            # Load the CSV files from S3.
            .load(SOURCE_PATH)
    )

    # Add ingestion metadata.
    #
    # file_name:
    #   Captures the source file path using Spark's metadata column.
    #   This helps identify which file a particular record came from.
    #
    # ingest_datetime:
    #   Records when the data was ingested into the Bronze layer.
    #   Useful for auditing, troubleshooting, and tracking pipeline runs.
    df = (
        df
        .withColumn("file_name", col("_metadata.file_path"))
        .withColumn("ingest_datetime", current_timestamp())
    )
    
    return df