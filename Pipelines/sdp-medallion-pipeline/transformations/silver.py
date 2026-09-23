from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Silver Layer - AUTO CDC WITH SCD Type 2 - History Tracking

# - Silver Orders
dp.create_streaming_table(
    name = "silver_orders",
    comment="SCD Type 2 history of order CDC events from Bronze",
    expect_all_or_drop={
        "valid_order_id": "order_id is not null",
        "valid_customer_id": "customer_id is not null",
        "valid_amount": "amount > 0"
    }
)

dp.create_auto_cdc_flow(
    target = "silver_orders",
    source= "bronze_orders_cdc",
    keys = ["order_id"],
    sequence_by = "ts_ms",
    apply_as_deletes = "op = 'd' or __deleted = 'true'",
    stored_as_scd_type = "2",
    except_column_list =["_ingest_timestamp", "_source_file", "ts_ms", "op", "__deleted"]
    
)


dp.create_streaming_table(
    name = "silver_customers",
    comment="SCD Type 2 history of customer CDC events from Bronze",
    expect_all_or_drop={
        "valid_customer_id": "customer_id is not null",
        "valid_name": "customer_name is not null"
    }
)

dp.create_auto_cdc_flow(
    target = "silver_customers",
    source= "bronze_customers_cdc",
    keys = ["customer_id"],
    sequence_by = "ts_ms",
    apply_as_deletes = "op = 'd' or __deleted = 'true'",
    stored_as_scd_type = "2",
    except_column_list =["_ingest_timestamp", "_source_file", "ts_ms", "op", "__deleted"]
    
)