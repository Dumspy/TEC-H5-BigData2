"""Main script: run the real-time pipeline (transform + load only).

Spark runs as a background process that watches Input_dir on HDFS. Every new
file that the manual extract script (`uv run python -m src.extract`) drops
there is automatically transformed (Iris-setosa filter) and loaded to
Output_dir/streaming.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from . import config
from .stream import start_streaming


def main() -> None:
    """Start the Spark Streaming watcher over Input_dir and keep running."""

    session = (
        SparkSession.builder.appName("iris-streaming")
        .master(config.SPARK_MASTER_URL)
        # Resolve DataNodes by hostname (e.g. over Tailscale) instead of the
        # LAN address the DataNode registered with the NameNode.
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    )
    if config.SPARK_DRIVER_HOST:
        # Remote client mode: make the driver reachable from the cluster
        # workers by advertising this machine's Tailscale IP, on fixed ports.
        session = (
            session.config("spark.driver.host", config.SPARK_DRIVER_HOST)
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.driver.port", "41000")
            .config("spark.blockManager.port", "41001")
        )
    spark = session.getOrCreate()

    try:
        query = start_streaming(spark)
        print(f"Watching {config.hdfs(config.INPUT_DIR)} for new files", flush=True)
        print(
            f"Results are appended to {config.hdfs(config.STREAMING_OUTPUT_DIR)}",
            flush=True,
        )
        print("Run `python -m src.extract` in another terminal to feed data", flush=True)
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
