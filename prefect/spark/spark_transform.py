#!/usr/bin/env python

import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types
import os
import pandas as pd
#import findspark
#findspark.init()


HOME = os.environ['HOME']
GPS_PROJECT = "datatalksclub-375802"
DTC_PROJECT_BASE = "bandcamp_sales"
GCS_BUCKET = f"{DTC_PROJECT_BASE}_{GPS_PROJECT}"
SPARK_CONNECTOR_JAR = f"{HOME}/spark/gcs-connector-hadoop3-latest.jar"

credentials_location = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
#GCS_BUCKET = os.environ['GCS_BUCKET']

conf = SparkConf() \
        .setMaster('local[*]') \
        .setAppName('test') \
        .set("spark.jars", SPARK_CONNECTOR_JAR) \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)


sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")


spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

print(GCS_BUCKET)
print("----------------------\n\n\n")
bcS_df = spark.read.parquet(f'gs://{GCS_BUCKET}/data/sales/*')

print("Parquet read from GCS.")


def id_udf(bc_id):
    """ 
    UDF function to cut out unnecessary bulk of id column
    """
    return bc_id.split("&//")[0]

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
        .select('intId', '_id', 'datetime', 'country_code', 'country', 'ST', 'IT', 'item_price', 'amount_paid_usd', 'currency', 'AOF', 'art_id', 'releases', 'artist_name', 'album_title')


"""
Find the non-distinct intId's
Since there are only 10 that make up 20 total sales,
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



"""
The crux of our issue here.
Create an iterator id for each country code so we can partition in BQ
"""
groupBy_cc = bcS_df \
        .groupBy('country_code') \
        .count() \
        .select(F.col('count'), 'country_code') \
        .orderBy(F.col('count').desc())

"""
Create DataFrame lookup table
"""
ccRef_df = groupBy_cc.rdd.zipWithIndex().map(lambda x: (x[0][0],x[0][1],x[1])).toDF(groupBy_cc.columns+["cc_ref"])


"""
Create the transormed table
"""
bcS_result = bcS_df.join(ccRef_df, bcS_df.country_code == ccRef_df.country_code) \
        .select(bcS_df.intId, bcS_df._id, bcS_df.datetime, bcS_df.country_code, bcS_df.country, bcS_df.ST, bcS_df.IT, bcS_df.item_price, bcS_df.amount_paid_usd, bcS_df.currency, bcS_df.AOF, bcS_df.art_id, bcS_df.releases, bcS_df.artist_name, bcS_df.album_title, ccRef_df.cc_ref)

bcS_result.coalesce(8) \
        .write \
        .csv("data/sales/bc_sales.csv", mode = "overwrite")

os.system("rm data/sales/bc_sales.csv/_SUCCESS")
