"""Load module: store the transformed DataFrame as one CSV file on HDFS."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession

from . import config


def load(
    spark: SparkSession,
    dataframe: DataFrame,
    output_path: str = config.hdfs(config.OUTPUT_FILE),
) -> str:
    """Write the transformed DataFrame as a single CSV file on HDFS.

    Spark normally writes a directory of part files. To end up with one
    visible CSV file (with column headers) at Output_dir/transform_iris.csv,
    the data is written to a temporary directory with coalesce(1) and the
    single part file is then renamed to the final name.

    The old output file is deleted first, so every run overwrites the old
    data instead of appending to it.
    """

    # The Hadoop FileSystem API is reached through Py4J (spark._jvm), which
    # is dynamically typed, hence the Any annotations.
    jvm: Any = spark._jvm
    jsc: Any = spark._jsc
    if jsc is None:
        raise RuntimeError("The Spark JVM gateway is not available")
    hadoop_conf: Any = jsc.hadoopConfiguration()
    hadoop_path: Any = jvm.org.apache.hadoop.fs.Path

    # Get the FileSystem for the output path itself (HDFS), not the default
    # one, which would be the local file system when HADOOP_CONF_DIR is unset.
    fs: Any = hadoop_path(output_path).getFileSystem(hadoop_conf)

    temporary_dir = f"{output_path}.tmp"
    fs.delete(hadoop_path(temporary_dir), True)
    fs.delete(hadoop_path(output_path), False)

    (
        dataframe.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(temporary_dir)
    )

    part_files = [
        status.getPath().toUri().getPath()
        for status in fs.listStatus(hadoop_path(temporary_dir))
        if status.getPath().getName().startswith("part-")
    ]
    if len(part_files) != 1:
        raise RuntimeError(f"Expected one part file, found {len(part_files)}")

    fs.rename(hadoop_path(part_files[0]), hadoop_path(output_path))
    fs.delete(hadoop_path(temporary_dir), True)
    return output_path
