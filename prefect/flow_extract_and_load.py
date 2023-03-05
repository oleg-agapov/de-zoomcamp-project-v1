import argparse
import requests
import pandas as pd
from io import BytesIO
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(timeout_seconds=60, retries=3)
def extract_from_web(url: str) -> bytes:
    """
    Task that downloads the file in bytes format from a given URL
    """
    r = requests.get(url)
    return r.content


@task()
def save_to_cloud_storage(content: bytes, storage_path: str) -> None:
    """
    Task saves given bytes content to the Cloud Storage
    """
    gcs = GcsBucket.load('de-zoomcamp-project')
    gcs.upload_from_file_object(BytesIO(content), storage_path)


@flow(name='Get raw data from Github archive')
def get_raw_and_save(date: str) -> None:
    """
    A complete Flow that can download a single date of Github Archive and save it to the target location.
    
    Keyword arguments:
    - date: the date in a string format "YYYY-DD-MM"
    """

    for hour in range(24):
        # parameters
        file_name = f'{date}-{hour}.json.gz'
        year, month, day = date.split('-')
        base_url = 'https://data.gharchive.org/'
        url = base_url + file_name
        target_folder = f"github_raw_data/{year}/{month}/{day}"
        target_full_path = f"{target_folder}/{file_name}"

        # main flow
        content = extract_from_web(url)
        save_to_cloud_storage(content, target_full_path)

        import gc
        del(content)
        gc.collect()


@flow(name='Main EL process')
def extract_and_load(
        start_date: str,  
        end_date: str = None
    ) -> None:
    """
    Main Extract-Load Flow for Github Data.
    """
    dates_range = [start_date]
    if end_date:
        dates_range = pd.date_range(start=start_date, end=end_date)
    for date in dates_range:
        get_raw_and_save(date=str(date)[:10])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Github Archive data')

    parser.add_argument('--start_date', help='Target date for processing')
    parser.add_argument('--end_date', required=False, help='End date (to upload a range of dates)')

    args = parser.parse_args()
    
    extract_and_load(
        start_date=args.start_date,
        end_date=args.end_date,
    )
