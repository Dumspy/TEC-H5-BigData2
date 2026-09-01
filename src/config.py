"""Shared configuration for the ETL pipeline."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import unquote, urlsplit

# Data source (flora data)
SOURCE_URL = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/iris.csv"

# The source file has no header row. The data scientist explanation of the
# columns gives us these names, which we prepend after download.
COLUMNS = ("sepal_length", "sepal_width", "petal_length", "petal_width", "species")
HEADERS = ",".join(COLUMNS)

# Transform filter: only Iris-setosa rows are kept.
SPECIES_COLUMN = "species"
SPECIES_FILTER = "Iris-setosa"

# Cluster settings. HDFS_HOST defaults to localhost (running on the cluster
# itself); set it to the tailscale host (e.g. big-data) to run from elsewhere.
HDFS_HOST = os.environ.get("HDFS_HOST", "localhost")
HDFS_USER = os.environ.get("HDFS_USER", "nixos")

# Spark's HDFS client authenticates with the local OS user. Use the cluster
# user instead so it has write access to /user/<HDFS_USER>.
os.environ.setdefault("HADOOP_USER_NAME", HDFS_USER)
HDFS_URL = os.environ.get("HDFS_URL", f"hdfs://{HDFS_HOST}:9000")  # for PySpark paths
WEBHDFS_URL = os.environ.get("WEBHDFS_URL", f"http://{HDFS_HOST}:9870")  # for the hdfs client
BASE_DIR = os.environ.get("HDFS_BASE_DIR", f"/user/{HDFS_USER}")

INPUT_DIR = f"{BASE_DIR}/Input_dir"
OUTPUT_DIR = f"{BASE_DIR}/Output_dir"

# Real-time pipeline: Spark Streaming monitors Input_dir for new files and
# appends results to STREAMING_OUTPUT_DIR. The checkpoint tracks which input
# files are already processed, so a restart skips them.
STREAMING_OUTPUT_DIR = f"{OUTPUT_DIR}/streaming"
CHECKPOINT_DIR = f"{BASE_DIR}/.checkpoint"

# File names are derived from the download URL, never hardcoded. The extract
# script appends a timestamp suffix, because Spark Streaming only reacts to
# NEW files, never to a file overwritten with the same name.
FILENAME = Path(unquote(urlsplit(SOURCE_URL).path)).name
INPUT_FILE = f"{INPUT_DIR}/{FILENAME}"
OUTPUT_FILE = f"{OUTPUT_DIR}/transform_{FILENAME}"

# Master of the installed Apache Spark on the cluster.
SPARK_MASTER_URL = os.environ.get("SPARK_MASTER_URL", f"spark://{HDFS_HOST}:7077")

# When the pipeline runs outside the cluster (client mode), the executors must
# be able to connect back to the driver. Set SPARK_DRIVER_HOST to this
# machine's tailscale IP so the cluster workers can reach it.
SPARK_DRIVER_HOST = os.environ.get("SPARK_DRIVER_HOST", "")


def hdfs(path: str) -> str:
    """Turn a plain HDFS path into a fully qualified path for PySpark."""
    return f"{HDFS_URL}{path}"
