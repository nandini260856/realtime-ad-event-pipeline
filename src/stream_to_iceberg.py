"""Stream ad events from Kafka into an Iceberg table."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

WAREHOUSE = "warehouse"
TABLE = "local.ads.impressions"

SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("campaign_id", StringType()),
    StructField("placement", StringType()),
    StructField("user_hash", StringType()),
    StructField("event_type", StringType()),
    StructField("bid_cpm", DoubleType()),
])

def build_spark():
    return (
        SparkSession.builder.appName("ads-kafka-to-iceberg")
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

def create_table(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.ads")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            event_id STRING,
            event_time TIMESTAMP,
            campaign_id STRING,
            placement STRING,
            user_hash STRING,
            event_type STRING,
            bid_cpm DOUBLE,
            event_date DATE
        )
        USING iceberg
        PARTITIONED BY (event_date, campaign_id)
        TBLPROPERTIES ('write.target-file-size-bytes'='134217728')
    """)

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    create_table(spark)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "ad-events")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 2000)
        .load()
    )

    events = (
        raw.select(from_json(col("value").cast("string"), SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_date", to_date(col("event_time")))
        .dropDuplicates(["event_id"])
    )

    query = (
        events.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", "checkpoints/impressions")
        .option("fanout-enabled", "true")
        .trigger(processingTime="30 seconds")
        .toTable(TABLE)
    )
    query.awaitTermination()

if __name__ == "__main__":
    main()