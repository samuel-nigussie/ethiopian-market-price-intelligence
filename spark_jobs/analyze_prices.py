"""
Spark batch analysis job.
Reads all archived price events from HDFS and computes:
  1. Average price by commodity
  2. Average price by commodity per year (inflation signal)
  3. Average price by region (regional comparison)
  4. Most volatile commodities (by coefficient of variation)
Writes results to local CSV files under output/results/.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, count, year, to_date

HDFS_INPUT_PATH = "hdfs://localhost:9000/market-prices/*/*.json"
LOCAL_OUTPUT_DIR = "output/results"


def main():
    spark = (
        SparkSession.builder
        .appName("EthiopiaMarketPriceAnalysis")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading data from {HDFS_INPUT_PATH} ...")
    df = spark.read.json(HDFS_INPUT_PATH)

    print(f"Loaded {df.count()} events.")
    df = df.withColumn("price_year", year(to_date(col("price_date"))))

    # 1. Average price by commodity
    avg_by_commodity = (
        df.groupBy("commodity")
        .agg(
            avg("price_close").alias("avg_price"),
            count("*").alias("event_count"),
        )
        .orderBy(col("avg_price").desc())
    )
    print("\n=== Average price by commodity ===")
    avg_by_commodity.show(truncate=False)
    avg_by_commodity.toPandas().to_csv(f"{LOCAL_OUTPUT_DIR}/avg_price_by_commodity.csv", index=False)

    # 2. Average price by commodity per year (inflation signal over time)
    avg_by_commodity_year = (
        df.groupBy("commodity", "price_year")
        .agg(avg("price_close").alias("avg_price"))
        .orderBy("commodity", "price_year")
    )
    print("\n=== Average price by commodity per year (sample) ===")
    avg_by_commodity_year.show(20, truncate=False)
    avg_by_commodity_year.toPandas().to_csv(f"{LOCAL_OUTPUT_DIR}/avg_price_by_commodity_year.csv", index=False)

    # 3. Average price by region
    avg_by_region = (
        df.groupBy("region", "commodity")
        .agg(avg("price_close").alias("avg_price"))
        .orderBy("region", "commodity")
    )
    print("\n=== Average price by region + commodity (sample) ===")
    avg_by_region.show(20, truncate=False)
    avg_by_region.toPandas().to_csv(f"{LOCAL_OUTPUT_DIR}/avg_price_by_region.csv", index=False)

    # 4. Volatility: coefficient of variation (std dev / mean) per commodity
    volatility = (
        df.groupBy("commodity")
        .agg(
            avg("price_close").alias("mean_price"),
            stddev("price_close").alias("std_price"),
        )
        .withColumn("coefficient_of_variation", col("std_price") / col("mean_price"))
        .orderBy(col("coefficient_of_variation").desc())
    )
    print("\n=== Commodity volatility (coefficient of variation) ===")
    volatility.show(truncate=False)
    volatility.toPandas().to_csv(f"{LOCAL_OUTPUT_DIR}/commodity_volatility.csv", index=False)

    print(f"\nAll results written to {LOCAL_OUTPUT_DIR}/")
    spark.stop()


if __name__ == "__main__":
    main()




