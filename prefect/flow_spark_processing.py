import argparse
import datetime

from prefect import flow, task
from prefect_gcp import GcsBucket, GcpCredentials
from google.cloud import dataproc_v1 as dataproc


def submit_job(project_id, region, cluster_name, python_file, target_date):
    #gcp = GcpCredentials.load('de-zoomcamp-project')
    import os

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/prefect/google.json"
    
    job_client = dataproc.JobControllerClient(
        #credentials=gcp.get_credentials_from_service_account(),
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )
    current_timestamp = round(datetime.datetime.now().timestamp())
    job = {
        "placement": {
            "cluster_name": cluster_name
        },
        "reference": {
            "job_id": f"job-{target_date}---{current_timestamp}",
            "project_id": project_id
        },
        "pyspark_job": {
            "main_python_file_uri": python_file,
            "properties": {},
            "args": [
                f"--target_date={target_date}"
            ]
        }
    }
    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    return operation.result()


@task()
def get_latest_date() -> str:
    gcs = GcsBucket.load('de-zoomcamp-project')
    folders = gcs.list_folders()
    folders = sorted([x for x in folders if x.startswith('github_processed_data/')])
    
    #last_day = datetime.datetime.strptime(folders[-1][-10:], '%Y/%m/%d')
    last_day = datetime.datetime(
        year=int(folders[-1].split('/')[1].split('=')[1]),
        month=int(folders[-1].split('/')[2].split('=')[1]),
        day=int(folders[-1].split('/')[3].split('=')[1]),
    )
    now = datetime.datetime.utcnow() - datetime.timedelta(hours=1)

    if last_day.date() < now.date():
        new_date = last_day + datetime.timedelta(days=1)
        return new_date.strftime('%Y-%m-%d')
    else:
        return now.strftime('%Y-%m-%d')


@task(log_prints=True)
def process_raw_data(
        project_id: str,
        region: str,
        cluster: str,
        python_file: str,
        target_date: str,
    ) -> None:
    print('Processing day:', target_date)
    result = submit_job(project_id, region, cluster, python_file, target_date)
    print(result)


@flow(name='Main flow')
def main_flow(
        project_id: str,
        region: str,
        cluster: str,
        python_file: str,
        target_date: str = None
    ) -> None:
    if target_date:
        date = target_date
    else:
        date = get_latest_date()
    process_raw_data(project_id, region, cluster, python_file, date)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process raw Github data')
    parser.add_argument('--project_id', help='GCP project ID')
    parser.add_argument('--region', help='GCP region')
    parser.add_argument('--cluster', help='DataProc cluster name')
    parser.add_argument('--python_file', help='PySpark file to submit')
    parser.add_argument('--target_date', required=False, help='Target date for processing')
    args = parser.parse_args()

    main_flow(
        args.project_id,
        args.region,
        args.cluster,
        args.python_file,
        args.target_date,
    )
