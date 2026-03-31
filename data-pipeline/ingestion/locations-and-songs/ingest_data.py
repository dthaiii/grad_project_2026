# -*- coding: utf-8 -*-
"""
Ingests location and song data to GCS bucket
"""
import argparse
import gzip
import io
import logging
import zipfile

import pyarrow.csv
import pyarrow.parquet
import requests

from google.cloud import storage

LOCATION_DATA_URL = "https://github.com/cpwill01/eventsim/raw/refs/heads/main/data/US.txt"
LOCATION_COLUMN_NAMES = ["Country Code", "Postal Code", "City", "State", "State Code",
                "Borough/County", "Borough/County Code","NA","NA","lat","long","acc"]
LOCATION_KEEP_COLS = ["Country Code", "Postal Code", "City", "State", "State Code",
                "Borough/County", "Borough/County Code"]
LOCATION_OUTFILE_NAME = "raw/flat_files/locations.parquet"

SONG_DATA_URL = "https://github.com/cpwill01/eventsim/raw/refs/heads/main/data/listen_counts.txt.gz"
SONG_COLUMN_NAMES = ["Track ID", "Artist", "Title", "Duration", "Listen Count", "Genre"]
SONG_KEEP_COLS = ["Track ID", "Artist", "Title", "Duration", "Genre"]
SONG_OUTFILE_NAME = "raw/flat_files/songs.parquet"

logger = logging.getLogger(__name__)

def ingest_data(url, col_names, keep_cols, bucket_name, outfile_name, compression=None,
        source_file_name=None, source_file_delimiter=b"\t"):

    # download location zip file
    r = requests.get(url)
    r.raise_for_status()
    logger.info(f"File downloaded from {url}")
    
    # extract and convert file to parquet, then upload
    if compression == "zip":
        z = zipfile.ZipFile(io.BytesIO(r.content))
        with z.open(source_file_name) as f:
            f = convert_to_parquet(f, source_file_delimiter, col_names, keep_cols)
    elif compression == "gzip":
        with gzip.open(io.BytesIO(r.content)) as f:
            f = convert_to_parquet(f, source_file_delimiter, col_names, keep_cols)
    else:
        f = convert_to_parquet(io.BytesIO(r.content), source_file_delimiter, col_names, keep_cols)
    upload_to_gcs(bucket_name, f, outfile_name)
    logger.info(f"Saved file to gs://{bucket_name}/{outfile_name}")

def upload_to_gcs(bucket_name, source_file, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_file(source_file)

def convert_to_parquet(f, delimiter, column_names, include_columns):
    parse_options = pyarrow.csv.ParseOptions(delimiter=delimiter)
    read_options = pyarrow.csv.ReadOptions(column_names=column_names)
    convert_options = pyarrow.csv.ConvertOptions(include_columns=include_columns)
    tbl = pyarrow.csv.read_csv(f, parse_options=parse_options, read_options=read_options,
                                convert_options=convert_options)
    result = io.BytesIO()
    pyarrow.parquet.write_table(tbl, result)
    result.seek(0)
    return result

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Ingest location and song data into GCS bucket")

    parser.add_argument("bucket_name", help="name of gcs bucket to save to")

    args = parser.parse_args()
    
    stream_handler = logging.StreamHandler("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    stream_handler.setFormatter(logging.Formatter())
    logger.addHandler(stream_handler)
    logger.info("Arguments received: " + args.__repr__())

    ingest_data(LOCATION_DATA_URL, LOCATION_COLUMN_NAMES, LOCATION_KEEP_COLS, args.bucket_name,
                LOCATION_OUTFILE_NAME)
    
    ingest_data(SONG_DATA_URL, SONG_COLUMN_NAMES, SONG_KEEP_COLS, args.bucket_name, SONG_OUTFILE_NAME,
                compression="gzip")