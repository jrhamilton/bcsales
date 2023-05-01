import os
import json
from prefect_sqlalchemy import SqlAlchemyConnector, ConnectionComponents, SyncDriver
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.credentials import GcpCredentials
from prefect_gcp.bigquery import BigQueryWarehouse
from prefect.infrastructure import DockerContainer

gcs_bucket=os.environ["GCS_BUCKET"]

# gac = GOOGLE_APPLICATION_CREDENTIALS
gac_file = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
gac = open(gac_file, "rb")

# sai = service account info
sai = json.load(gac)

os.system('prefect block register -m prefect_gcp')

credentials = GcpCredentials(
    service_account_info=sai
)
credentials.save("gcp-credentials", overwrite=True)

GcsBucket(
    bucket=gcs_bucket,
    gcp_credentials=credentials,
    bucket_folder=''
).save('gcs-bucket', overwrite=True)

BigQueryWarehouse(
    gcp_credentials=credentials,
).save("bq-warehouse", overwrite=True)

DockerContainer(
    image="dochamilton/bcsales:v001",
    image_pull_policy="NEVER",
    auto_renew=True,
).save("bcsales-ingest-container", overwrite=True)
