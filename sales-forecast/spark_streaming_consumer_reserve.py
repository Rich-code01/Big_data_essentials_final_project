"""
PySpark Structured Streaming Consumer.

Reads live sales transactions from the 'sales-stream' Kafka topic and:
  1. Prints each raw record to the console as it arrives.
  2. Maintains and prints running aggregate totals (revenue + units) by
     region and category, updated as new data streams in.

Usage:
    python spark_streaming_consumer.py

Leave the kafka_producer.py running in another terminal (or having already
run it earlier with earliest offsets) to see data flow through.

Requires: HDFS running (not strictly needed here, but keep it up for
the rest of the pipeline), Kafka running, topic 'sales-stream' existing.

First run will download the Spark-Kafka connector jar via Maven
(needs internet access, one-time, cached afterward).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as spark_sum
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "sales-stream"

# Must match the fields the producer sends (see generate_sales_data.py / kafka_producer.py)
SALES_SCHEMA = StructType([
    StructField("transaction_id", IntegerType()),
    StructField("date", StringType()),
    StructField("day_of_week", StringType()),
    StructField("store_id", StringType()),
    StructField("region", StringType()),
    StructField("product_id", StringType()),
    StructField("category", StringType()),
    StructField("units_sold", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("promotion_flag", IntegerType()),
    StructField("discount_pct", IntegerType()),
    StructField("sales_amount", DoubleType()),
])


def main():
    print("Starting Spark session (this will download the Kafka connector on first run)...")
    spark = (
        SparkSession.builder
        .appName("SalesStreamingConsumer")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Read the raw stream from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Kafka gives us key/value as bytes; cast value to string, then parse the JSON
    parsed_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), SALES_SCHEMA).alias("data"))
        .select("data.*")
    )

    print(f"\nListening to topic '{KAFKA_TOPIC}'...\n")
    print("=" * 70)
    print("Two live views will print below:")
    print("  1) RAW RECORDS as they arrive")
    print("  2) RUNNING TOTALS by region + category (updates as data streams)")
    print("=" * 70 + "\n")

    # ---- Query 1: print raw records as they arrive ----
    raw_query = (
        parsed_stream.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 10)
        .queryName("raw_records")
        .start()
    )

    # ---- Query 2: running aggregate totals by region + category ----
    aggregated_stream = (
        parsed_stream.groupBy("region", "category")
        .agg(
            spark_sum("sales_amount").alias("total_revenue"),
            spark_sum("units_sold").alias("total_units_sold"),
        )
        .orderBy(col("total_revenue").desc())
    )

    agg_query = (
        aggregated_stream.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 20)
        .queryName("running_totals")
        .start()
    )

    print("Streaming started. Press Ctrl+C to stop.\n")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nStopping streams...")
        raw_query.stop()
        agg_query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
