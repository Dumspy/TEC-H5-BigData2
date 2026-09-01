"""Main script: run the Extract -> Transform -> Load pipeline."""

from __future__ import annotations

from pyspark.sql import SparkSession

from . import config
from .extract import extract
from .load import load
from .transform import transform


def main() -> None:
    """Extract the flora data to HDFS, transform it with Spark, and load it."""

    # Extract: download from the source directly into HDFS.
    input_path = extract()
    print(f"Extracted data to: {input_path}")

    # Transform + Load run with PySpark on the installed Apache Spark,
    # reading and writing on HDFS.
    session = SparkSession.builder.appName("iris-etl").master(config.SPARK_MASTER_URL)
    if config.SPARK_DRIVER_HOST:
        # Remote client mode: make the driver reachable from the cluster.
        session = (
            session.config("spark.driver.host", config.SPARK_DRIVER_HOST)
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.driver.port", "41000")
            .config("spark.blockManager.port", "41001")
        )
    spark = session.getOrCreate()

    try:
        transformed = transform(spark, config.hdfs(config.INPUT_FILE))
        print(f"Transformed rows (Iris-setosa): {transformed.count()}")

        output_path = load(spark, transformed)
        print(f"Loaded transformed data to: {output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
