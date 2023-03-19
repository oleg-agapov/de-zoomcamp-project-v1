from flow_extract_and_load import extract_and_load
from prefect.deployments import Deployment
from prefect.filesystems import GitHub


github_block = GitHub.load('de-zoomcamp-project')


deployment = Deployment.build_from_flow(
    flow=extract_and_load,
    name='Load raw data',
    storage=github_block,
)

if __name__ == '__main__':
    deployment.apply()
