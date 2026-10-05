"""
Step 3 & 4 & 5: Feature Engineering + MLlib Model Training + Store Predictions in MySQL

Reads the historical sales dataset from HDFS, aggregates it to daily total
revenue, engineers time-series features, trains a Random Forest Regressor
to forecast daily revenue, evaluates it, then writes actual vs. predicted
results into a MySQL table for the Django dashboard (Step 6) to display.

Usage:
    python train_and_predict.py

Requires: HDFS running with /project/sales_data/sales_data.csv present,
MySQL running with the 'sales_forecast' database created, and the
MySQL Connector/J jar available (path set below).
"""

import setuptools  # noqa: F401  (provides a distutils shim needed by pyspark.ml on Python 3.12+)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg as spark_avg, count as spark_count,
    to_date, dayofweek, month, when, lag, row_number
)
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# ---------------- Config ----------------
HDFS_PATH = "hdfs://localhost:9000/project/sales_data/sales_data.csv"
MYSQL_JAR_PATH = r"C:\spark-jars\mysql-connector-j-26.7.0.jar"

MYSQL_HOST = "localhost"
MYSQL_PORT = "3307"
MYSQL_DB = "sales_forecast"
MYSQL_USER = "root"
MYSQL_PASSWORD = "kamana"   # <-- update this before running
MYSQL_TABLE = "daily_sales_predictions"

TEST_SET_DAYS = 180  # most recent N days held out as the test set (time-based split)


def main():
    print("Starting Spark session...")
    spark = (
        SparkSession.builder
        .appName("SalesForecastTraining")
        .config("spark.jars", MYSQL_JAR_PATH)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ---------------- Load from HDFS ----------------
    print(f"Reading dataset from {HDFS_PATH} ...")
    df = spark.read.csv(HDFS_PATH, header=True, inferSchema=True)
    df = df.withColumn("date", to_date(col("date")))
    print(f"Loaded {df.count():,} transaction rows.")

    # ---------------- Step 3: Feature Engineering ----------------
    print("Aggregating to daily revenue and engineering features...")

    daily = (
        df.groupBy("date")
        .agg(
            spark_sum("sales_amount").alias("daily_revenue"),
            spark_sum("units_sold").alias("daily_units_sold"),
            spark_avg("promotion_flag").alias("promo_rate"),
            spark_avg("discount_pct").alias("avg_discount_pct"),
            spark_count("transaction_id").alias("transaction_count"),
        )
        .orderBy("date")
    )

    daily = (
        daily
        .withColumn("day_of_week_num", dayofweek(col("date")))          # 1=Sunday .. 7=Saturday
        .withColumn("month_num", month(col("date")))
        .withColumn("is_weekend", when(col("day_of_week_num").isin(1, 7), 1).otherwise(0))
        .withColumn("is_holiday_season", when(col("month_num").isin(11, 12), 1).otherwise(0))
    )

    # Lag features: yesterday's revenue, and a 7-day rolling average (time-series signal)
    date_window = Window.orderBy("date")
    rolling_window = Window.orderBy("date").rowsBetween(-7, -1)

    daily = (
        daily
        .withColumn("prev_day_revenue", lag("daily_revenue", 1).over(date_window))
        .withColumn("rolling_7day_avg_revenue", spark_avg("daily_revenue").over(rolling_window))
    )

    # Drop the first few rows where lag/rolling features are null (no prior history yet)
    daily = daily.na.drop(subset=["prev_day_revenue", "rolling_7day_avg_revenue"])

    total_days = daily.count()
    print(f"Daily aggregated dataset ready: {total_days:,} days of data (after dropping warm-up rows).")

    # ---------------- Step 4: Train/Test Split (time-based) + Model Training ----------------
    daily = daily.orderBy("date")
    daily_with_idx = daily.withColumn("idx", row_number().over(date_window) - 1)
    split_point = total_days - TEST_SET_DAYS

    train_df = daily_with_idx.filter(col("idx") < split_point).drop("idx")
    test_df = daily_with_idx.filter(col("idx") >= split_point).drop("idx")

    print(f"Train set: {train_df.count():,} days | Test set: {test_df.count():,} days "
          f"(most recent {TEST_SET_DAYS} days held out)")

    feature_cols = [
        "day_of_week_num", "month_num", "is_weekend", "is_holiday_season",
        "promo_rate", "avg_discount_pct", "transaction_count",
        "daily_units_sold", "prev_day_revenue", "rolling_7day_avg_revenue",
    ]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    train_vec = assembler.transform(train_df)
    test_vec = assembler.transform(test_df)

    print("Training Random Forest Regressor...")
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="daily_revenue",
        numTrees=100,
        maxDepth=8,
        seed=42,
    )
    model = rf.fit(train_vec)

    # ---------------- Evaluate ----------------
    predictions = model.transform(test_vec)

    evaluator_rmse = RegressionEvaluator(labelCol="daily_revenue", predictionCol="prediction", metricName="rmse")
    evaluator_mae = RegressionEvaluator(labelCol="daily_revenue", predictionCol="prediction", metricName="mae")
    evaluator_r2 = RegressionEvaluator(labelCol="daily_revenue", predictionCol="prediction", metricName="r2")

    rmse = evaluator_rmse.evaluate(predictions)
    mae = evaluator_mae.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)

    print("\n" + "=" * 50)
    print("MODEL EVALUATION (on held-out test days)")
    print("=" * 50)
    print(f"RMSE : {rmse:,.2f}")
    print(f"MAE  : {mae:,.2f}")
    print(f"R2   : {r2:.4f}")
    print("=" * 50 + "\n")

    # Feature importances (useful for the report's "Findings" section)
    print("Feature importances:")
    for name, importance in sorted(
        zip(feature_cols, model.featureImportances.toArray()),
        key=lambda x: -x[1]
    ):
        print(f"  {name:30s} {importance:.4f}")

    # ---------------- Step 5: Store Predictions in MySQL ----------------
    print(f"\nWriting predictions to MySQL table '{MYSQL_TABLE}'...")

    results = predictions.select(
        col("date"),
        col("daily_revenue").alias("actual_revenue"),
        col("prediction").alias("predicted_revenue"),
        col("day_of_week_num"),
        col("is_weekend"),
        col("is_holiday_season"),
    )

    jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"

    (
        results.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", MYSQL_TABLE)
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .mode("overwrite")
        .save()
    )

    print(f"Done. {results.count():,} prediction rows written to "
          f"{MYSQL_DB}.{MYSQL_TABLE}")

    spark.stop()


if __name__ == "__main__":
    main()
