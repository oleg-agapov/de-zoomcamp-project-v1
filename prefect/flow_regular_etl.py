import requests
from io import BytesIO
import datetime

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task()
def get_latest_date() -> str:
    gcs = GcsBucket.load('de-zoomcamp-project')
    folders = gcs.list_folders()
    folders = sorted([x for x in folders if x.startswith('github_raw_data/')])
    
    last_day = datetime.datetime.strptime(folders[-1][-10:], '%Y/%m/%d')
    now = datetime.datetime.utcnow()

    if last_day.date() < now.date():
        new_date = last_day + datetime.timedelta(days=1)
        return new_date.strftime('%Y-%m-%d')
    else:
        return now.strftime('%Y-%m-%d')


@task(timeout_seconds=60, retries=3)
def extract_from_web(url: str) -> bytes:
    """
    Task that downloads the file in bytes format from a given URL
    """
    r = requests.get(url)
    return r.content


@task(timeout_seconds=60, retries=3)
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
    now = datetime.datetime.utcnow()
    now_date = now.date().strftime('%Y-%m-%d')
    now_hour = now.hour

    for hour in range(24):
        if date == now_date:
            if hour >= now_hour:
                continue
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


@flow(name='Main periodic EL process')
def extract_and_load() -> None:
    date = get_latest_date()
    get_raw_and_save(date=date)


if __name__ == '__main__':
    extract_and_load()
