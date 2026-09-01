"""Real-time transform and load: Spark Streaming watches Input_dir.

The streaming query runs as a background process. Every time the extract
script drops a new file into Input_dir on HDFS, Spark automatically picks it
up, filters out everything except Iris-setosa (transform), and appends the
result to Output_dir/streaming (load). No extract code runs here.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from . import config

SCHEMA = StructType(
    [
        StructField("sepal_length", DoubleType(), False),
        StructField("sepal_width", DoubleType(), False),
        StructField("petal_length", DoubleType(), False),
        StructField("petal_width", DoubleType(), False),
        StructField("species", StringType(), False),
    ]
)


def start_streaming(spark: SparkSession):
    """Start the Input_dir watcher and return the streaming query."""

    return (
        spark.readStream.option("header", "true")
        .schema(SCHEMA)
        .csv(config.hdfs(config.INPUT_DIR))
        .filter(F.col(config.SPECIES_COLUMN) == config.SPECIES_FILTER)
        .writeStream.format("csv")
        .outputMode("append")
        .option("path", config.hdfs(config.STREAMING_OUTPUT_DIR))
        .option("checkpointLocation", config.hdfs(config.CHECKPOINT_DIR))
        .trigger(processingTime="10 seconds")
        .start()
    )
