from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name = "gold_daily_revenue",
    comment="Daily revenue by customer tier"
)

def gold_daily_revenue():

    orders = (
        spark.read.table("silver_orders")
        .filter(F.col("__END_AT").isNull())
        .filter(F.col("status") == "completed")
    )
    customers = (
        spark.read.table("silver_customers")
        .filter(F.col("__END_AT").isNull())
        .select("customer_id", "customer_tier")
    )
    
    return (
        orders.join(customers, on="customer_id", how="left")
        .groupBy(F.to_date("order_date").alias("order_date"), F.col("customer_tier"))
        .agg(
            F.sum("amount").alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.avg("amount").alias("avg_revenue")
        ).orderBy("order_date", "customer_tier")

    )
    