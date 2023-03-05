from flow_extract_and_load import extract_and_load
from prefect.deployments import Deployment
from prefect.filesystems import GitHub


github_block = GitHub.load('de-zoomcamp-project')


deployment = Deployment.build_from_flow(
    flow=extract_and_load,
    name='Extract and Load Github raw data',
    storage=github_block,
    parameters={'start_date': '2023-01-02'},
)

if __name__ == '__main__':
    deployment.apply()
