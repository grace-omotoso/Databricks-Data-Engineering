from pyspark import pipelines as dp
from pyspark.sql.functions import col

#### Bronze Layer - raw CDC event landing

ORDERS_LANDING_PATH = "/Volumes/dev/dbx_course/landing/orders"
CUSTOMERS_LANDING_PATH = "/Volumes/dev/dbx_course/landing/customers"
SCHEMA_LOCATION_BASE = "/Volumes/dev/dbx_course/landing/_schema"


@dp.table(
    name="bronze_orders_cdc",
    comment="Raw CDC events for orders - append-only landing table"
)
@dp.expect("op_is_valid", "op in ('c', 'u', 'd')")
@dp.expect_or_drop("ts_ms_not_null", "ts_ms is not null")
def bronze_orders_cdc():

    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", True)
            .option(
                "cloudFiles.schemaLocation",
                SCHEMA_LOCATION_BASE + "/orders"
            )
            .option("header", "true")
            .load(ORDERS_LANDING_PATH)
            .select(
                "*",
                col("_metadata.file_modification_time").alias("_ingest_timestamp"),
                col("_metadata.file_path").alias("_source_file")
            )
)
    
@dp.table(
    name="bronze_customers_cdc",
    comment="Raw CDC events for customers - append-only landing table"
)
@dp.expect("op_is_valid", "op in ('c', 'u', 'd')")
@dp.expect_or_drop("ts_ms_not_null", "ts_ms is not null")
def bronze_customers_cdc():

    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", True)
            .option(
                "cloudFiles.schemaLocation",
                SCHEMA_LOCATION_BASE + "/customers"
            )
            .option("header", "true")
            .load(CUSTOMERS_LANDING_PATH)
            .select(
                "*",
                col("_metadata.file_modification_time").alias("_ingest_timestamp"),
                col("_metadata.file_path").alias("_source_file")
            )
)
    
