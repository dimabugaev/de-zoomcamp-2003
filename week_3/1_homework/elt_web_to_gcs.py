from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import os



@task(log_prints=True)
def download_to_local_file(url_path: str, local_file_name: str) -> Path:
    """Download taxi data locally"""
    Path(f'data/').mkdir(parents=True, exist_ok=True)
    os.system(f'wget {url_path} -O data/{local_file_name}')
    return Path(f'data/{local_file_name}')


@task()
def write_to_gcs(path: Path) -> None:
    """Upload data file to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("gcsbucket-hw3")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=f"{path}", to_path=path.name)


@flow(log_prints=True)
def etl_web_to_gcs(month: int = 1, year: int = 2020) -> None:
    """Load data for one month"""

    dataset_file = f'fhv_tripdata_{year}-{month:02}'
    print(dataset_file)

   
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz'
    print(dataset_url)

    path = download_to_local_file(dataset_url, f'{dataset_file}.csv.gz')
    write_to_gcs(path)

@flow()
def etl_web_to_gcs_main(year: int = 2019) -> None:
    """The main ETL function"""
    months = [x for x in range(1,12 + 1)]
    #months = [2]
    
    for month in months:
        #print(month)
        etl_web_to_gcs(month, year)

if __name__ == '__main__':
    etl_web_to_gcs_main()