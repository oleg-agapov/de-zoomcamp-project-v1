from flow_extract_and_load import extract_and_load
from prefect import get_client
from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from prefect_gcp.cloud_run import CloudRunJob

#client = get_client()

github_block = GitHub.load('de-zoomcamp-project')
# cloud_run_job_block = CloudRunJob.load('de-zoomcamp-project')


deployment = Deployment.build_from_flow(
    flow=extract_and_load,
    name='Extract and Load Github raw data',
    storage=github_block,
    #infrastructure=cloud_run_job_block,
)

if __name__ == '__main__':
    deployment.apply()
