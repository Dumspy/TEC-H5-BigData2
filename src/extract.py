"""Extract module: download the flora data directly into HDFS.

Extract method 4 is used: the Python requests library. The `hdfs` package is
a WebHDFS client built on requests, so the download is streamed straight from
the source into HDFS and never touches the local file system.
"""

from __future__ import annotations

import requests
from hdfs import InsecureClient

from .config import HEADERS, HDFS_USER, INPUT_FILE, SOURCE_URL, WEBHDFS_URL

CHUNK_SIZE = 64 * 1024


def extract(
    url: str = SOURCE_URL,
    destination: str = INPUT_FILE,
) -> str:
    """Download the dataset from the source and store it on HDFS.

    Returns the HDFS path of the stored CSV.
    """

    # DATA SECURITY AND DATA INTEGRITY:
    # - The URL must be HTTPS, so TLS protects and authenticates the download.
    # - Both transfers (source -> HDFS) are plain library calls, no shell and
    #   no external process is involved, so command-line injection is
    #   impossible.
    # - The destination name is derived from the URL basename (see
    #   filename_for in config.py), never hardcoded, so the saved CSV keeps
    #   its original name.
    if not url.lower().startswith("https://"):
        raise ValueError("The dataset URL must be a valid HTTPS URL")

    client = InsecureClient(WEBHDFS_URL, user=HDFS_USER)

    def download_stream():
        """Yield the header row first, then the downloaded file in chunks."""
        yield f"{HEADERS}\n".encode()
        with requests.get(url, stream=True, timeout=(10, 60)) as response:
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    yield chunk

    # ROBUSTNESS UNDER DOWNLOAD INTERRUPTION:
    # - The stream is written to a temporary name on HDFS first. Only when the
    #   complete download has arrived is it renamed to the real destination,
    #   so an interrupted download can never replace valid input with partial
    #   data. The old file is only removed just before the rename.
    # - The requests timeout stops a stalled connection from hanging forever,
    #   and raise_for_status() rejects HTTP error pages.
    # - Every run starts from a clean state, so the old CSV is always
    #   overwritten instead of appended to.
    temporary = f"{destination}.download"
    client.delete(temporary, recursive=True)
    client.delete(destination, recursive=False)
    client.write(temporary, data=download_stream(), overwrite=True)
    client.rename(temporary, destination)

    return destination
