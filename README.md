# Iris ETL Pipeline (HDFS / Spark)

A minimal big data pipeline with Extract, Transform and Load (ETL) that runs
against the installed Hadoop/HDFS and Apache Spark cluster.

```text
GitHub HTTPS source (iris.csv)
        |
        v
src/extract.py    Extract: requests streams the download straight into HDFS
        |
        v
HDFS /user/<user>/Input_dir/iris.csv        (column headers prepended)
        |
        v
src/transform.py  PySpark filter: species == Iris-setosa
        |
        v
src/load.py       Load: HDFS /user/<user>/Output_dir/transform_iris.csv
```

## Project structure

```text
src/
├── __init__.py
├── main.py        # runs Extract -> Transform -> Load
├── config.py      # shared constants (URL, columns, HDFS paths)
├── extract.py     # download source -> HDFS (never touches local disk)
├── transform.py   # PySpark filter
└── load.py        # transformed DataFrame -> one CSV file on HDFS
```

## Requirements

- Python with `uv` (devenv)
- The cluster's HDFS (`hdfs://localhost:9000`) with WebHDFS enabled (port 9870)
- The installed Apache Spark (`spark://localhost:7077`)
- PySpark runs on the installed Spark on top of DFS, not the local Python setup

## Configuration

Environment variables (defaults match the cluster setup):

| Variable         | Default                            |
|------------------|------------------------------------|
| `HDFS_HOST`      | `localhost` (use `big-data` remotely) |
| `HDFS_USER`      | `nixos`                            |
| `HDFS_BASE_DIR`  | `/user/<HDFS_USER>`                |
| `HDFS_URL`       | `hdfs://<HDFS_HOST>:9000`          |
| `WEBHDFS_URL`    | `http://<HDFS_HOST>:9870`          |
| `SPARK_MASTER_URL` | `spark://<HDFS_HOST>:7077`       |

## Run against the cluster over Tailscale

The cluster host is `big-data`. Once HDFS and Spark are running there and
ports 9000/7077 are reachable, run from anywhere:

```bash
HDFS_HOST=big-data uv run python -m src.main
```

If the Spark master or NameNode RPC is not reachable over Tailscale, run on
the cluster itself over SSH, where `HDFS_HOST` stays `localhost`:

```bash
ssh big-data
cd big-data-2 && uv run python -m src.main
```

## Run

```bash
uv sync
uv run python -m src.main
```

Or submit it through Spark on the cluster:

```bash
spark-submit --master spark://localhost:7077 src/main.py
```

Each run overwrites the old files on HDFS:

- `Input_dir/iris.csv` — source data with column headers
- `Output_dir/transform_iris.csv` — only Iris-setosa rows, with headers

## Implementation notes

### Extract (`src/extract.py`)

Uses **Extract method 4: Python `requests`**. The `hdfs` package is a WebHDFS
client built on requests, so the download is streamed in chunks directly into
HDFS — the data never touches the local file system, and no shell or external
process is involved (no command-line injection risk). The destination name is
derived from the download URL basename, never hardcoded. The column header row
is prepended in the same stream.

Robustness under download interruption: the stream is written to a temporary
HDFS file and only renamed to the real destination after the complete download
has arrived. The requests timeout stops a stalled connection from hanging
forever, and `raise_for_status()` rejects HTTP error pages.

### Transform (`src/transform.py`)

PySpark reads the CSV from HDFS with an explicit schema and keeps only rows
where `species == Iris-setosa`.

### Load (`src/load.py`)

Spark writes the transformed DataFrame with `coalesce(1)` to a temporary HDFS
directory and renames the single part file to
`Output_dir/transform_iris.csv`, so one named CSV file with headers exists at
the end. The old output file is deleted first, so every run overwrites it.
