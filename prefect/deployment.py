#from test_flow import my_docker_flow

import os
#from prefect.deployments import Deployment
from prefect_sqlalchemy import SqlAlchemyConnector, ConnectionComponents, SyncDriver
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.credentials import GcpCredentials
from prefect.infrastructure import DockerContainer

do_docker=True
service_account_file = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
docker_service_account_file = '/opt/prefect/.gac.json'
gcp_bucket="bandcamp_sales_datatalksclub-375802"
saf = docker_service_account_file if do_docker else service_account_file

print("PRINTING CREDENTIALS ****")
print(saf)

if False:
    connector = SqlAlchemyConnector(
        connection_info=ConnectionComponents(
            driver=SyncDriver.POSTGRESQL_PSYCOPG2,
            username="root",
            password="root",
            host="pgdatabase",
            port=5432,
            database="bandcamp_dev",
        )
    )
    connector.save("pg-database", overwrite=True)


os.system('prefect block register -m prefect_gcp')


credentials = GcpCredentials(
    service_account_file=service_account_file
)
credentials.save("dtc-credentials-2", overwrite=True)


GcsBucket(
    bucket=gcp_bucket,
    gcp_credentials=credentials,
    bucket_folder='/'
).save('bandcamp-dev-2', overwrite=True)


if do_docker:
    DockerContainer(
        image="dochamilton/bcsales:v001",
        image_pull_policy="NEVER",
        auto_renew=True,
    ).save("bc-sales-1", overwrite=True)


if False:
    minio_block = RemoteFileSystem(
        basepath="s3://prefect-flows/test_flow",
        key_type="hash",
        settings=dict(
            use_ssl = False,
            key = "blablabla",
            secret = "blablabla",
            client_kwargs = dict(endpoint_url = "http://minio:9000")
        ),
    )
    minio_block.save("minio", overwrite=True)


    deployment = Deployment.build_from_flow(
        name="docker-example",
        flow=my_docker_flow,
        storage=RemoteFileSystem.load('minio'),
        infrastructure=DockerContainer(
            image = 'prefect-orion:2.4.5',
            image_pull_policy = 'IF_NOT_PRESENT',
            networks = ['prefect'],
            env = {
                "USE_SSL": False,
                "AWS_ACCESS_KEY_ID": "blablabla",
                "AWS_SECRET_ACCESS_KEY": "blablabla",
                "ENDPOINT_URL": 'http://minio:9000',
            }
        ),
    )
    deployment.apply()
