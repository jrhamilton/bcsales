from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from flows.ingest_sales_flow import parametized_flow

docker_block = DockerContainer.load("bc-sales-1")

docker_dep = Deployment.build_from_flow(
    flow=parametized_flow,
    name='docker-flow-2',
    infrastructure=docker_block
)

if __name__ == "__main__":
    docker_dep.apply()
