import os
import sys
import json
from dotenv import load_dotenv
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


def read_json(path: str) -> dict:
    with open(path, 'r') as f:
        return json.load(f)


def create_gcp_credentials_block(block_name: str, service_account_info: dict) -> None:
    block = GcpCredentials(
        service_account_info=service_account_info
    )
    block.save(block_name, overwrite=True)


def create_storage_block(block_name: str, bucket_name: str, gcp_block_name: str) -> None:
    block = GcsBucket(
        gcp_credentials=GcpCredentials.load(gcp_block_name),
        bucket=bucket_name,
    )
    block.save(block_name, overwrite=True)


if __name__ == '__main__':
    
    load_dotenv()

    service_account_path = os.getenv('GCP_SERVICE_ACCOUNT_PATH')
    bucket_name = os.getenv('GCS_BUCKET_NAME')
    if not service_account_path:
        sys.exit('Create GCP_SERVICE_ACCOUNT_PATH variable in ".env"')
    gcp_service_info = read_json(service_account_path)

    create_gcp_credentials_block('de-zoomcamp-project', gcp_service_info)
    create_storage_block('de-zoomcamp-project', bucket_name, 'de-zoomcamp-project')
