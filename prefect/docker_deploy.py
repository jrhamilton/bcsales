from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from flows.ingest_data_flow import ingest_flow
from flows.gcp_to_bq import etl_gcp_to_bq

docker_block = DockerContainer.load("bcsales-ingest-container")

docker_dep1 = Deployment.build_from_flow(
    flow=ingest_flow,
    name='docker-flow-to-gcs',
    infrastructure=docker_block
)

docker_dep2 = Deployment.build_from_flow(
    flow=etl_gcp_to_bq,
    name='docker-flow-to-bq',
    infrastructure=docker_block
)

if __name__ == "__main__":
    docker_dep1.apply()
    docker_dep2.apply()
