from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.view(
    name="trips_silver_staging", comment="Transformed trips data ready for CDC upsert"
)
@dp.expect("valid_date", "year(business_date) >= 2020")
@dp.expect("valid_driver_rating", "driver_rating BETWEEN 1 AND 10")
@dp.expect("valid_passenger_rating", "passenger_rating BETWEEN 1 AND 10")
def trips_silver():
    df_bronze = spark.readStream.table("transportation.bronze.trips")
    df_silver = df_bronze.withColumn("passenger_type", F.lower("passenger_type"))

    df_silver = df_bronze.select(
        F.col("trip_id").alias("id"),
        F.col("date").cast("date").alias("business_date"),
        F.col("city_id").alias("city_id"),
        F.col("passenger_type").alias("passenger_category"),
        F.col("distance_travelled_km").alias("distance_kms"),
        F.col("fare_amount").alias("sales_amt"),
        F.col("passenger_rating").alias("passenger_rating"),
        F.col("driver_rating").alias("driver_rating"),
        F.col("ingest_datetime").alias("bronze_ingest_timestamp"),
    )

    df_silver = df_silver.withColumn(
        "silver_processed_timestamp", F.current_timestamp()
    )
    return df_silver


dp.create_streaming_table(
    name="transportation.silver.trips",
    comment="Cleaned and validated orders with CDC upsert capability",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)

dp.create_auto_cdc_flow(
    target="transportation.silver.trips",
    source="trips_silver_staging",
    keys=["id"],
    sequence_by=F.col("silver_processed_timestamp"),
    stored_as_scd_type=1,
    except_column_list=[],
)

# Slowly Changing Dimensions (SCD) are techniques used in data warehousing to manage and track changes in dimension data over time. They help maintain historical records while ensuring accurate reporting and analysis. SCD is commonly used for dimensions such as customers, products, employees, and locations.

# Type 0 (Fixed Dimension): The dimension is immutable; values are set once and never changed. Examples include date of birth or original registration date.
# Type 1 (Overwrite): The old value is replaced with the new value, and no history is maintained. This approach is suitable when historical information is not required.
# Type 2 (Add New Row): A new record is created for each change, typically using surrogate keys, effective dates, and current-record indicators. This preserves complete historical data.
# Type 3 (Add New Attribute): Additional columns are added to store previous values. This allows limited historical tracking, usually only the current and previous values.
# Type 4 (History Table): Current records are stored in the main dimension table, while historical records are maintained in a separate history table.
# Type 5 (Mini-Dimension with Type 1): Combines Type 4 and Type 1. Historical changes are tracked in a mini-dimension table, while current values are maintained in the main dimension table for easier reporting.
# Type 6 (Hybrid / Type 1+2+3): Combines features of Types 1, 2, and 3. New rows are created to preserve history (Type 2), previous values may be stored in additional columns (Type 3), and some attributes can be overwritten (Type 1).


