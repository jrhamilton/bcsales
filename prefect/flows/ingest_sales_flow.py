#!/usr/bin/env python
# coding: utf-8

import sys
sys.path
import os
from pathlib import Path
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


#print(os.environ['PREFECT_API_URL'])

@task(log_prints=True, retries=3)
def file_extract(url, filename):
    if url is None:
        print(f"Filename IS: {filename}")
        f = filename
        if csv_name.endswith('csv.gz'):
            csv_name = 'output.gz'
            compression = 'gzip'
        else:
            csv_name = 'output.csv'
            compression = None
    else:
        print(f"URL IS: {url}")
        f = url
        if url.endswith('csv.gz'):
            csv_name = 'output.gz'
            compression = 'gzip'
        elif url.endswith('csv'):
            csv_name = 'output.csv'
            compression = None
        else:
            print("This is a zip file just because I already know.")
            csv_name = 'output.zip'
            compression = 'zip'

        #os.system(f"wget {url} -O {csv_name}")

    df = pd.read_csv(f, compression=compression)

    return (df, csv_name)


@task()
def extract_data(csv_name, compression):
    df = pd.read_csv(csv_name, compression=compression)
    return df


@task(log_prints=True)
def transform_data(df):
    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame, filename: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    filename_base = filename.split('.')[0]
    dir_path = 'data/sales'
    Path(dir_path).mkdir(parents=True, exist_ok=True)
    path = Path(f"{dir_path}/{filename_base}.parquet")
    print(f"THE LOCAL PATH IS::::>>>> {path}")
    df.to_parquet(path)
    l = len(df)
    print(f"Rows: {l}")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load('bandcamp-dev-2')
    gcs_block.upload_from_path(
            from_path=f"{path}",
            to_path=path
    )
    return


@flow(name="Meta Type", log_prints=True)
def log_subflow_how_file_obtained(url, filename):
    print("LOGGING SUBFOW: *******")
    print(f"url: {url}")
    print(f"filename: {filename}")

    if url==None and filename==None:
        print("Both URL and Filename were not supplied. Must exit")
        raise ValueError("Must supply either URL or Filename. Exiting")

    if url != None:
        f = "URL"
    else:
        f = "Local File"

    print(f"Logging for Subflow capturing file obtained by: {f}")


@flow(name="Main")
def parametized_flow(url: str = None, filename: str = None):
        
    log_subflow_how_file_obtained(url, filename)

    df, csv_name = file_extract(url, filename)
    #raw_df = extract_data(csv_name, compression)
    #df = transform_data(raw_df)
    #load_data(table_name, raw_data)
    path = write_local(df, csv_name)
    write_gcs(path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest csv data to Postgres')

    parser.add_argument('--url', help='url of the csv file if by url')
    parser.add_argument('--filename', help='filename of csv file if by local file')

    args = parser.parse_args()

    parametized_flow(args.url, args.filename)
