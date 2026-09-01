"""Transform module: filter the extracted data with PySpark on the cluster."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from . import config


def transform(
    spark: SparkSession,
    input_path: str = config.hdfs(config.INPUT_FILE),
) -> DataFrame:
    """Read the extracted CSV from HDFS and keep only Iris-setosa rows."""

    schema = StructType(
        [
            StructField("sepal_length", DoubleType(), False),
            StructField("sepal_width", DoubleType(), False),
            StructField("petal_length", DoubleType(), False),
            StructField("petal_width", DoubleType(), False),
            StructField("species", StringType(), False),
        ]
    )

    source = (
        spark.read.option("header", "true")
        .option("mode", "FAILFAST")
        .schema(schema)
        .csv(input_path)
    )

    # Keep only rows where species == Iris-setosa.
    return (
        source.filter(F.col(config.SPECIES_COLUMN) == config.SPECIES_FILTER)
        .select(*config.COLUMNS)
    )
