import os
from flow_spark_processing import main_flow
from prefect.deployments import Deployment
from prefect.filesystems import GitHub


github_block = GitHub.load('de-zoomcamp-project')


deployment = Deployment.build_from_flow(
    flow=main_flow,
    name='Process raw data with Spark',
    storage=github_block,
    parameters={
        'project_id': os.getenv('GCP_PROJECT_ID'),
        'region': os.getenv('GCP_REGION'),
        'cluster_name': 'dataproc2',
        'python_file': 'gs://dataproc-de-zoomcamp-2023/notebooks/jupyter/process_github.py',
    }
)

if __name__ == '__main__':
    deployment.apply()
