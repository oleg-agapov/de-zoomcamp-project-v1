import os
from flow_spark_processing import main_flow
from prefect.deployments import Deployment
from prefect.filesystems import GitHub


github_block = GitHub.load('de-zoomcamp-project')


deployment = Deployment.build_from_flow(
    flow=main_flow,
    name='Process raw data',
    storage=github_block,
    parameters={
        'project_id': 'de-zoomcamp-2023-project',
        'region': 'europe-west6',
        'cluster': 'dataproc2',
        'python_file': 'gs://dataproc-de-zoomcamp-2023/notebooks/jupyter/process_github.py',
    }
)

if __name__ == '__main__':
    deployment.apply()
