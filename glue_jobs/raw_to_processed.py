"""
Glue ETL Job: Raw → Processed → Curated
- Reads raw NDJSON clickstream + order events from S3
- Deduplicates and cleans data
- Writes Parquet to processed zone
- Aggregates daily KPIs to curated zone
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_BUCKET", "PROCESSED_BUCKET", "CURATED_BUCKET"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW = f"s3://{args['RAW_BUCKET']}"
PROCESSED = f"s3://{args['PROCESSED_BUCKET']}"
CURATED = f"s3://{args['CURATED_BUCKET']}"

# ── 1. Read Raw Clickstream ────────────────────────────────────────────────────
click_raw = (
    spark.read
    .option("recursiveFileLookup", "true")
    .json(f"{RAW}/clickstream/")
)

if click_raw.count() == 0:
    print("No clickstream data found, skipping.")
else:
    click_clean = (
        click_raw
        .dropDuplicates(["event_id"])
        .filter(F.col("event_id").isNotNull())
        .withColumn("event_date", F.to_date("timestamp"))
        .withColumn("event_hour", F.hour("timestamp"))
        .withColumn("is_authenticated", F.col("customer_id").isNotNull())
        .withColumn("processed_at", F.current_timestamp())
    )

    (
        click_clean.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"{PROCESSED}/clickstream/")
    )
    print(f"Clickstream: wrote {click_clean.count()} records to processed zone")

# ── 2. Read and Deduplicate Orders ────────────────────────────────────────────
# Orders come in via DynamoDB exports — simulate reading from raw bucket
try:
    orders_raw = (
        spark.read
        .option("recursiveFileLookup", "true")
        .json(f"{RAW}/orders/")
    )

    orders_clean = (
        orders_raw
        .dropDuplicates(["order_id"])
        .filter(F.col("order_id").isNotNull() & F.col("total").isNotNull())
        .withColumn("order_date", F.to_date("created_at"))
        .withColumn("total", F.col("total").cast("double"))
        .withColumn("fraud_score", F.col("fraud_score").cast("double"))
        .withColumn("processed_at", F.current_timestamp())
    )

    (
        orders_clean.write
        .mode("overwrite")
        .partitionBy("order_date")
        .parquet(f"{PROCESSED}/orders/")
    )
    print(f"Orders: wrote {orders_clean.count()} records to processed zone")

    # ── 3. Daily KPI Aggregation → Curated ───────────────────────────────────
    daily_kpis = (
        orders_clean
        .groupBy("order_date")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total").alias("gross_revenue"),
            F.avg("total").alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum(F.when(F.col("status") == "confirmed", 1).otherwise(0)).alias("confirmed_orders"),
            F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).alias("failed_orders"),
            F.avg("fraud_score").alias("avg_fraud_score"),
            F.sum(F.when(F.col("fraud_score") >= 0.5, 1).otherwise(0)).alias("flagged_orders"),
        )
        .withColumn("conversion_rate", F.col("confirmed_orders") / F.col("total_orders"))
        .withColumn("fraud_rate", F.col("flagged_orders") / F.col("total_orders"))
        .orderBy("order_date")
    )

    (
        daily_kpis.write
        .mode("overwrite")
        .parquet(f"{CURATED}/daily_kpis/")
    )
    print(f"Daily KPIs: wrote {daily_kpis.count()} date partitions to curated zone")

    # ── 4. Customer Lifetime Value (CLV) aggregation ──────────────────────────
    clv = (
        orders_clean
        .filter(F.col("status") == "confirmed")
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("total").alias("total_spend"),
            F.avg("total").alias("avg_order_value"),
            F.min("created_at").alias("first_order"),
            F.max("created_at").alias("last_order"),
            F.countDistinct("order_date").alias("active_days"),
        )
        .withColumn("clv_segment", F.when(F.col("total_spend") > 1000, "high")
                    .when(F.col("total_spend") > 300, "medium")
                    .otherwise("low"))
    )

    (
        clv.write
        .mode("overwrite")
        .parquet(f"{CURATED}/customer_clv/")
    )
    print(f"CLV: wrote {clv.count()} customer records to curated zone")

except Exception as e:
    print(f"Orders processing skipped: {e}")

# ── 5. Product Affinity Matrix (for recommendations training) ─────────────────
try:
    orders_for_affinity = spark.read.parquet(f"{PROCESSED}/orders/")

    # Explode order items to get product co-purchases
    items_exploded = orders_for_affinity.select(
        "order_id",
        "customer_id",
        F.explode("items").alias("item"),
    ).select(
        "order_id",
        "customer_id",
        F.col("item.product_id").alias("product_id"),
        F.col("item.category").alias("category"),
        F.col("item.unit_price").alias("unit_price"),
        F.col("item.quantity").alias("quantity"),
    )

    # Customer-product interaction matrix (for collaborative filtering)
    interactions = (
        items_exploded
        .groupBy("customer_id", "product_id")
        .agg(
            F.count("order_id").alias("purchase_count"),
            F.sum("quantity").alias("total_quantity"),
            F.sum(F.col("quantity") * F.col("unit_price")).alias("total_spend"),
        )
        .withColumn("implicit_rating", F.log1p(F.col("purchase_count") * F.col("total_quantity")))
    )

    (
        interactions.write
        .mode("overwrite")
        .parquet(f"{CURATED}/product_interactions/")
    )
    print(f"Product interactions: wrote {interactions.count()} records to curated zone")

except Exception as e:
    print(f"Product affinity skipped: {e}")

job.commit()
print("Glue ETL job completed successfully.")
