#!/usr/bin/env python

import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types
import os
import pandas as pd
#from prefect import flow, task


HOME = os.environ['HOME']
GCP_PROJECT = os.environ['GCP_PROJECT']
DTC_PROJECT_BASE = "bandcamp_sales"
GCS_BUCKET = f"{DTC_PROJECT_BASE}_{GCP_PROJECT}"
SPARK_CONNECTOR_JAR = f"{HOME}/spark/gcs-connector-hadoop3-latest.jar"
CREDENTIALS_LOCATION = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
DATA_PATH = f"{HOME}/bcsales/prefect/spark/data"


conf = SparkConf() \
        .setMaster('local[*]') \
        .setAppName('test') \
        .set("spark.jars", SPARK_CONNECTOR_JAR) \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", CREDENTIALS_LOCATION)

sc = SparkContext(conf=conf)
hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", CREDENTIALS_LOCATION)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

SPARK = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()



#@task(log_prints=True, retries=3)
def read_bucket_parquet():
    print(f"Reading fromm {GCS_BUCKET}")
    bcS_df = SPARK.read.parquet(f'gs://{GCS_BUCKET}/data/lake/*')

    print("Parquet read from GCS.")
    print("BandCamp Sales DataFrame created.")
    return bcS_df


@F.udf
def replace_quotes(text):
    if text==None:
        return text

    text = text.replace('\"', '*')

    return text


def id_udf(bc_id):
    """ 
    UDF function to cut out unnecessary bulk of id column
    """
    if "&" in bc_id:
        return bc_id.split("&")[0]
    else:
        return bc_id


#@task(log_prints=True, retries=3)
def transform_data(bcS_df):

    bcS_df = bcS_df \
            .withColumn("album_title", replace_quotes(F.col('album_title'))) \
            .withColumn("artist_name", replace_quotes(F.col('artist_name')))

    do_id_udf = F.udf(id_udf, returnType=types.StringType())
        
    """
    Select only necessary columns.
    Implement the intId column transformed by the UDF.
    Transform some columns into shorter descriptions.
    """
    with_intIds_df = bcS_df \
            .withColumn('intId', do_id_udf(bcS_df._id)) \
            .withColumn('datetime', F.to_date(bcS_df.utc_date)) \
            .withColumn('ST', F.lit('slug_type')) \
            .withColumn('IT', F.lit('item_type')) \
            .withColumn('AOF', F.lit('amount_over_format')) \
            .select('intId', '_id', 'datetime', 'country_code',
                    'country', 'ST', 'IT', 'item_price',
                    'amount_paid_usd','currency', 'AOF',
                    'art_id', 'releases', 'artist_name', 'album_title')

    print("Sales DataFrame transformed.")
    print("Bulky Id shortened.")
    print("Datetime formatted.")
    print("Uneccessary columns removed.")
    return with_intIds_df


#@task(log_prints=True, retries=3)
def return_sales_with_only_distinct_ids(with_intIds_df):
    """
    Find the non-distinct intId's.
    Since there are only 10 non-distinct id's
    that make up 20 total sales out of a million,
    We are going to exclude them as anomalies.
    """
    nondistinct_intIds_df = with_intIds_df  \
            .groupBy('intId') \
            .count() \
            .filter(F.col('count') > 1) \
            .select('intId')

    """
    Then the next 3 methods, we collect them into a list
    Then filter out the non-distinct intId's from the original DataFrame
    """
    collect = nondistinct_intIds_df.collect()

    collect_array = [row.intId for row in collect]

    bcS_df = with_intIds_df.filter(~with_intIds_df.intId.isin(collect_array))

    print("Non Distinct Id's removed from BandCamp Sales DataFrame.")
    return bcS_df


#@task(log_prints=True, retries=3)
def create_country_code_refs_dataframe(bcS_df):
    """
    The crux of our issue here.
    Create an iterator id for each country code so we can partition in BQ
    """
    groupBy_cc = bcS_df \
            .groupBy('country_code') \
            .count() \
            .select(F.col('count'), 'country_code', 'country') \
            .orderBy(F.col('count').desc())

    """
    Create DataFrame lookup table
    AKA, Country Code Reference DataFrame
    This gives us a BQ partitionable id for Country Codes
    The idea for iterating the DataFrame and attaching an id
    like the following was found on Stackoverflow
    """
    ccRef_df = groupBy_cc.rdd.zipWithIndex() \
            .map(lambda x: (x[0][0],x[0][1],x[0][2],x[1])) \
            .toDF(groupBy_cc.columns+["cc_ref"])

    print("Country Code Reference DataFrame created.")
    return ccRef_df


#@task(log_prints=True, retries=3)
def output_to_csv(bcS_df, ccRef_df):
    """
    Create the transformed table
    Join the the new 'cc_ref' country code id
    """
    bcS_result = bcS_df \
            .join(ccRef_df, bcS_df.country_code == ccRef_df.country_code) \
            .select(bcS_df.intId, bcS_df._id, bcS_df.datetime, bcS_df.country_code,
                    bcS_df.ST, bcS_df.IT, bcS_df.item_price,
                    bcS_df.amount_paid_usd, bcS_df.currency, bcS_df.AOF,
                    bcS_df.art_id, bcS_df.releases, bcS_df.artist_name, bcS_df.album_title,
                    ccRef_df.cc_ref)

    print("Writing final BandCamp Sales to CSV file.")
    bcS_result.coalesce(8) \
            .write \
            .csv(f"{DATA_PATH}/sales/bc_sales.csv", header=True, mode = "overwrite")

    print("Writing Country Code Reference to CSV file.")
    ccRef_df.write \
            .csv(f"{DATA_PATH}/refs/country_code_ref.csv", header=True, mode="overwrite")

    print("Clean up")
    os.system(f"rm {DATA_PATH}/sales/bc_sales.csv/_SUCCESS")
    os.system(f"rm {DATA_PATH}/refs/country_code_ref.csv/_SUCCESS")


#@flow(name="Spark-Flow")
def spark_flow():
    bcS_df = read_bucket_parquet()
    bcS_df = transform_data(bcS_df)
    bcS_df = return_sales_with_only_distinct_ids(bcS_df)
    ccR_df = create_country_code_refs_dataframe(bcS_df)
    output_to_csv(bcS_df, ccR_df)


if __name__ == "__main__":
    """ BEGIN SPARK FLOW """
    spark_flow()
