"""
Kafka Producer: reads the sales dataset from HDFS and streams it into the
'sales-stream' Kafka topic, simulating real-time transactions arriving one
at a time.

Usage:
    python kafka_producer.py --rate 100          (100 rows/sec)
    python kafka_producer.py --rate 1000         (faster)
    python kafka_producer.py --rate 0            (no delay, max speed)
    python kafka_producer.py --limit 50000       (only stream first 50,000 rows, useful for quick tests)
    python kafka_producer.py --rate 200 --limit 20000
Requires: pyspark, kafka-python (already installed in bigdata-env)
Requires: HDFS running, Kafka running, topic 'sales-stream' already created
"""

import argparse
import datetime
import decimal
import json
import time

from kafka import KafkaProducer
from pyspark.sql import SparkSession


def json_default(obj):
    """Handles types that Spark can return (date, datetime, Decimal) which
    Python's default json encoder doesn't know how to serialize."""
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

HDFS_PATH = "hdfs://localhost:9000/project/sales_data/sales_data.csv"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "sales-stream"


def main():
    parser = argparse.ArgumentParser(description="Stream sales data from HDFS into Kafka")
    parser.add_argument("--rate", type=float, default=100,
                         help="Rows per second to send (0 = no delay, max speed). Default: 100")
    parser.add_argument("--limit", type=int, default=None,
                         help="Only stream the first N rows (useful for quick tests). Default: all rows")
    args = parser.parse_args()

    delay = 0 if args.rate <= 0 else 1.0 / args.rate

    print(f"Starting producer -> topic '{KAFKA_TOPIC}' at {args.rate} rows/sec"
          f"{' (no delay)' if delay == 0 else ''}")
    if args.limit:
        print(f"Limiting to first {args.limit:,} rows")

    print("Starting Spark session to read from HDFS...")
    spark = SparkSession.builder.appName("SalesKafkaProducer").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.csv(HDFS_PATH, header=True, inferSchema=True)
    if args.limit:
        df = df.limit(args.limit)

    total_rows = df.count()
    print(f"Dataset loaded from HDFS: {total_rows:,} rows. Beginning stream...\n")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=json_default).encode("utf-8"),
    )

    sent = 0
    start_time = time.time()

    try:
        # toLocalIterator streams rows from the Spark driver one at a time,
        # instead of pulling the entire dataset into memory at once.
        for row in df.toLocalIterator():
            record = row.asDict()
            producer.send(KAFKA_TOPIC, value=record)
            sent += 1

            if sent % 1000 == 0:
                elapsed = time.time() - start_time
                actual_rate = sent / elapsed if elapsed > 0 else 0
                print(f"Sent {sent:,} / {total_rows:,} rows "
                      f"(actual rate: {actual_rate:.1f} rows/sec)")

            if delay > 0:
                time.sleep(delay)

    except KeyboardInterrupt:
        print("\nStopped by user (Ctrl+C).")

    finally:
        producer.flush()
        producer.close()
        elapsed = time.time() - start_time
        print(f"\nDone. Sent {sent:,} rows in {elapsed:.1f} seconds "
              f"({sent / elapsed if elapsed > 0 else 0:.1f} rows/sec average).")
        spark.stop()


if __name__ == "__main__":
    main()
