import os
from pathlib import Path
import pandas as pd
#from prefect import flow, task
from prefect_gcp.bigquery import BigQueryWarehouse


GCP_PROJECT = os.environ["GCP_PROJECT"]
DTC_PROJECT_BASE = "bandcamp_sales"
BUCKET = f"{DTC_PROJECT_BASE}_{GCP_PROJECT}"
PROJECT_SCHEMA = f"{DTC_PROJECT_BASE}_schema"
BQ_BLOCK = "bq-warehouse"


#@task(log_prints=True, retries=3)
def initialize_ext_table():
    with BigQueryWarehouse.load(BQ_BLOCK) as warehouse:
        """ CREATE TABLES """
        print("Initializing External bc_sales_ext table.")

        create_bc_sales_ext = f"""
            CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT}.{PROJECT_SCHEMA}.bc_sales_ext`
                OPTIONS (
                    format = 'PARQUET',
                    uris = ['gs://{BUCKET}/data/sales/sales*.parquet']
                );
        """
        warehouse.execute(create_bc_sales_ext)


#@task(log_prints=True, retries=3)
def initialize_ref_table():
    with BigQueryWarehouse.load(BQ_BLOCK) as warehouse:
        """ CREATE COUNTRY CODE REFERENCE TABLE """
        print("Creating Country Code Reference Table.")

        create_cc_ref_ext = f"""
            CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT}.{PROJECT_SCHEMA}.country_code_ref`
                OPTIONS (
                    format = 'PARQUET',
                    uris = ['gs://{BUCKET}/data/refs/refs.parquet']
                );
        """
        warehouse.execute(create_cc_ref_ext)


#@task(log_prints=True, retries=3)
def get_cc_ref_range_max():
    """
    Get the max cc_ref from the country_code_ref table
    so that we know the range for RANGE_BUCKET
    """
    with BigQueryWarehouse.load(BQ_BLOCK) as warehouse:
        get_range_max = f"""
            SELECT MAX(`cc_ref`) FROM `{GCP_PROJECT}.{PROJECT_SCHEMA}.country_code_ref`
        """

        range_max = warehouse.fetch_one(get_range_max)[0]
        print(f"Max Country Code Reference is: {range_max}")
        return range_max


#@task(log_prints=True, retries=3)
def create_partitioned_table(rb_max):
    with BigQueryWarehouse.load(BQ_BLOCK) as warehouse:
        """
        CREATE sales PARTITIONED TABLE
        Using RANGE_BUCKET on cc_ref code.
        """
        print("Creating Partitioned sales table with RANGE_BUCKET on country_code reference.")

        create_bcS_currency_part_ext = f"""
            CREATE OR REPLACE TABLE `{GCP_PROJECT}.{PROJECT_SCHEMA}.sales_partitioned`
            PARTITION BY RANGE_BUCKET(cc_ref, GENERATE_ARRAY(0, {rb_max}, 1))  AS (
                SELECT * FROM `{GCP_PROJECT}.{PROJECT_SCHEMA}.bc_sales_ext`
            );
        """
        warehouse.execute(create_bcS_currency_part_ext)

               
#@task(log_prints=True, retries=3)
def create_cluster_by_artist_name(rb_max):
    with BigQueryWarehouse.load(BQ_BLOCK) as warehouse:
        """
        ADD CLUSTER BY artist_name
        """
        print("Creating cluster by artist_name on top of Partition.")

        create_bcS_currency_part_ext = f"""
            CREATE OR REPLACE TABLE `{GCP_PROJECT}.{PROJECT_SCHEMA}.sales_clustered`
            PARTITION BY RANGE_BUCKET(cc_ref, GENERATE_ARRAY(0, {rb_max}, 1))
            CLUSTER BY artist_name AS (
                SELECT * FROM `{GCP_PROJECT}.{PROJECT_SCHEMA}.sales_partitioned`
            );
        """
        warehouse.execute(create_bcS_currency_part_ext)


#@flow(name="Main_GCP_TO_BQ", log_prints=True)
def etl_gcs_to_bq():
    initialize_ext_table()
    initialize_ref_table()
    cc_ref_range_max = get_cc_ref_range_max()
    print(cc_ref_range_max)
    create_partitioned_table(cc_ref_range_max)
    create_cluster_by_artist_name(cc_ref_range_max)


if __name__ == "__main__":
    etl_gcs_to_bq()
