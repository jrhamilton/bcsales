#!/bin/bash

cd $HOME/bcsales/prefect

echo "BEGIN Prefect setup.."
python3 deployment.py

echo "docker_deploy no bueno.. That's OK. Docker not really necessary.."
#python3 docker_deploy.py

echo "Create DataFrame from our dataset.."
python3 ./flows/ingest_data_flow.py --url=https://www.dropbox.com/s/wd38q80el16i19q/1000000-bandcamp-sales.zip?dl=1 --category=lake

echo "Use PySpark to transform our sales data.."
echo "Use PySpark to create a Country Code Reference Table for our BigQuery RANGE_BUCKET."
python3 ./spark/spark_transform.py

echo "Ingest the new sales csv(s) and send back to the Data Lake.."
python3 ./flows/ingest_data_flow.py --directory=$HOME/bcsales/prefect/spark/data/sales/bc_sales.csv/ --category=sales

echo "Ingest our new Country Code Reference Table and send to the Data Lake.."
python3 ./flows/ingest_data_flow.py --directory=$HOME/bcsales/prefect/spark/data/refs/country_code_ref.csv/ --category=refs

echo "Create our Tables -- Basic table from Sales data, Reference Table and Partitioned and Clustered tables.."
echo "Our Partitioned table will be based off our Reference table using RANGE_BUCKET."
python3 ./flows/gcp_to_bq.py
