#!/usr/bin/env python
# coding: utf-8

import os
from pathlib import Path
import glob
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
#from prefect import flow, task
#from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
#from data_cache import pandas_cache

GCS_BLOCK = 'gcs-bucket'


#@task(log_prints=True)
def how_file_obtained(url, filename, bloburl, directory):
    """ Log how we obtained file and set meta accordingly """

    if url==None and filename==None and bloburl==None and directory==None:
        print("URL, Filename, BlobURL and Directory were not supplied. Must exit")
        raise ValueError("Must supply one of either URL, Filename, BlobURL or Directory. Exiting")

    if url != None:
        filetype = "URL"
        meta = 0
    elif filename != None:
        filetype = "Local File"
        meta = 1
    elif bloburl != None:
        filetype = "Blob URL"
        meta = 2
    else:
        filetype = "Local Directory"
        meta = 3

    print(f"Logging for Subflow capturing file obtained by: {filetype}")

    return meta


#@task(log_prints=True)
def get_compression_type(meta: int, f: str):
    if meta%2==0 and '?' in f:
        f = f.split('?')[0]

    if f.endswith('.parquet'):
        compression = [None,'parquet']
        meta = meta + 4
    elif f.endswith('csv'):
        compression = [None, 'csv']
    elif f.endswith('csv.gz'):
        compression = ['gzip', 'csv.gz']
    elif f.endswith('zip'):
        compression = ['zip', 'csv.zip']
    else:
        compression = None
        meta = 8

    print(f"META is: {meta}")
    print(f"COMPORESSION is: {compression[0]}")
    return meta, compression


#@task(log_prints=True)
def get_output(i: int):
    i = str(i)
    if i=='0':
        i = ''

    c = CATEGORY

    output_dir = f"data/{c}"
    output_file = f"{c}{i}.parquet"

    print(f"CATEGORY: {c}")
    print(f"OUTPUT_DIR: {output_dir}")
    print(f"OUTPUT_FILE: {output_file}")
    return output_dir, output_file


#@task(log_prints=True,
#      retries=3,
#      cache_key_fn=task_input_hash,
#      cache_expiration=timedelta(minutes=1))
#@pandas_cache
def read_csv_to_dataframe(f, compression, meta):
    """ Read file to DataFrame before preparing to write locally """
    print("Create DataFrame file then write to parquet file.")

    df = pd.read_csv(f, compression=compression[0])

    head = df.head()

    l = len(df)

    print(f"LINE COUNT: {l}")

    print("First 5 rows of DataFrame:")
    print(head)
    print("--------------------------")

    return (df)


#@task(log_prints=True)
def write_parquet_to_local(meta, f,
                           output_dir,
                           output_file):
    """ Write Parquet file to local path """
    print("Sending parquet file to output path.")

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    output_path = Path(f"{output_dir}/{output_file}")

    if meta%2==0:
        os.system(f"wget {f} -O {output_path}")
    else:
        os.system(f"cp {f} {output_path}")

    line_count = sum(1 for line in open(output_path))
    print(f"Rows: {line_count}")

    return output_path, line_count


#@task(log_prints=True)
def write_dataframe_to_local(df: pd.DataFrame,
                             output_dir: str,
                             output_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    print("Writing DataFrame to parquet output path.")

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    output_path = Path(f"{output_dir}/{output_file}")
    df.to_parquet(output_path)

    line_count = len(df)
    print(f"DataFrame Rows: {line_count}")

    return output_path, line_count


#@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    print("Uploading Parquet file to Bucket.")
    gcs_block = GcsBucket.load(GCS_BLOCK)
    gcs_block.upload_from_path(
            from_path=path,
            to_path=path
    )
    return


#@flow(name="URL Blob")
def url_blob(url):
    """ URL BLOB """
    # Finish later.
    # Not in use so may be left incomplete


#@flow(name="Directory Blob", log_prints=True)
def directory_blob(directory):
    """ Directory Blob """
    print("Start Directory loop.")
    print(f"DIRECTOY: {directory}")
    print("--------------------")

    for idx, path in enumerate(glob.glob(f"{directory}*")):
        print(f"filename: {path}")
        to_gcs_flow(3, path, idx)


#@flow(name="Blob Flow")
def loop_blob(meta, blob):
    """ More than one file to work with """
    if meta==2:
        url_blob(blob)
    else:
        directory_blob(blob)


#@flow(name="ToGCS", log_prints=True)
def to_gcs_flow(meta: int, f: str, i: int):
    """
    Heart of the flow
    Single file operation
    """
    # od = output_directory
    # of = output_file
    od, of = get_output(i)

    meta, compression = get_compression_type(meta, f)

    if meta == 8:
        return

    if meta > 3:
        # We have a parquet file.
        path, lc = write_parquet_to_local(meta, f, od, of)
    else:
        # Not a parquet file. Create DataFrame first.
        df = read_csv_to_dataframe(f, compression, meta)
        path, lc = write_dataframe_to_local(df, od, of)

    # Check if Line Count is zero.
    # This often happens when Spark writes files to directories.
    if lc==0:
        print('File has line count of zero. Skip.')
        return

    write_gcs(path)


#@flow(name="ToParquetToDataLake")
def ingest_flow(category: str,
                url: str = None,
                filename: str = None,
                bloburl: str = None,
                directory: str = None):

    globals()['CATEGORY'] = category
        
    meta = how_file_obtained(url, filename, bloburl, directory)

    if meta > 1:
        # Blob. Will be looping.
        blob = bloburl if meta==2 else directory
        loop_blob(meta, blob)
    else:
        # Single file. Go straight to to_gcs_flow()
        f = url if meta==0 else filename
        to_gcs_flow(meta, f, 0)
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest csv data to Postgres')

    parser.add_argument('--url', help='url of the csv file if by url')
    parser.add_argument('--filename', help='filename of csv file if by local file')
    parser.add_argument('--bloburl', help='url and directory file of url')
    parser.add_argument('--directory', help='directory of csv files')
    parser.add_argument('--category', help='category of data')

    args = parser.parse_args()

    if args.category == None:
        print("Catgory not set.")
        raise ValueError("Must set category. Exiting.")

    ingest_flow(args.category,
                args.url,
                args.filename,
                args.bloburl,
                args.directory)
                
