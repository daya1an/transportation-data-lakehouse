from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Define a materialized view for cleaned and standardized city dimension data.
@dp.materialized_view(
    name="transportation.silver.city",
    comment="Cleaned and standardized products dimension with business transformations",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def city_silver():
    # Load raw city data from bronze table
    df_bronze = spark.read.table("transportation.bronze.city")
    
    # Select and rename relevant columns for silver layer processing
    df_silver = df_bronze.select(
        F.col("city_id").alias("city_id"),
        F.col("city_name").alias("city_name"),
        F.col("ingest_datetime").alias("bronze_ingest_timestamp")
    )
    
    # Add a processing timestamp column to track silver layer transformations
    df_silver = df_silver.withColumn(
        "silver_processed_timestamp", F.current_timestamp()
    )

    # Return the cleaned and transformed DataFrame for materialized view creation
    return df_silver