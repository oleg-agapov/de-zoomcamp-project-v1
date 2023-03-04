import argparse
import requests
import pandas as pd
from io import BytesIO
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task()
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
def get_raw_and_save(date: str, hour: int) -> None:
    """
    A complete Flow that can download a single file from Github Archive and save it to the target location.
    
    Keyword arguments:
    - date: the date in a string format "YYYY-DD-MM"
    - hour: the hour, 0..23
    """
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


@flow(name='Main EL process')
def extract_and_load(
        mode: str, # {'bootstrap', 'batch'}
        start_date: str, 
        hour: int = None, 
        end_date: str = None
    ) -> None:
    """
    Main Extract-Load Flow for Github Data.

    Arguments:
    - mode: how the flow should operate
        - 'bootstrap' - accepts a date range and upload all the data from that range
        - 'batch' - accepts a single data and hour and upload it
    - start_date: either start date for 'bootstrap' mode or target date for 'batch' mode
    - hour: the hour to upload, only for 'batch' mode
    - end_date: the end date to upload, only for 'bootstrap' mode
    """
    if mode == 'bootstrap':
        # make dates range
        # and run for date and hour
        if not end_date:
            #sys.exit('Please provide end_date for the correct work')
            raise Exception('Please provide end_date for the correct work')
        date_range = pd.date_range(start=start_date, end=end_date)
        for date in date_range:
            for hour in range(24):
                get_raw_and_save(date=str(date)[:10], hour=hour)
    if mode == 'batch':
        # run for given hour
        get_raw_and_save(date=start_date, hour=hour)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Github Archive data')

    parser.add_argument('--mode', help='Mode in which the flow should be started', choices=['batch', 'bootstrap'])
    parser.add_argument('--start_date', help='Target date for processing')
    parser.add_argument('--end_date', required=False, help='End date for bootrstrap mode')
    parser.add_argument('--hour', required=False, help='Hour to load for batch mode')

    args = parser.parse_args()
    
    extract_and_load(
        mode=args.mode,
        start_date=args.start_date,
        end_date=args.end_date,
        hour=args.hour,
    )
