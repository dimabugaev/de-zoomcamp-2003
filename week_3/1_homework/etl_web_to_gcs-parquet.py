from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task()
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas dataFrame"""
    return pd.read_csv(dataset_url)

@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write taxi data locally as parquet file"""
    Path(f'data').mkdir(parents=True, exist_ok=True)
    path = Path(f"data/{dataset_file}.parquet")
    df.to_parquet(path, compression='gzip')
    return path

@task()
def write_to_gcs(path: Path) -> None:
    """Upload data file to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("gcsbucket-hw3")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=f"{path}", to_path=path.name)


@flow()
def etl_web_to_gcs(month: int = 1, year: int = 2019) -> None:
    """Load data for one month"""

    dataset_file = f'fhv_tripdata_{year}-{month:02}'
    print(dataset_file)
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz'
    print(dataset_url)

    df = fetch(dataset_url)
    path = write_local(df, dataset_file)
    write_to_gcs(path)


@flow()
def etl_web_to_gcs_main(year: int = 2019) -> None:
    """The main ETL function"""
    months = [x for x in range(1,12 + 1)]
    
    for month in months:
        etl_web_to_gcs(month, year)

if __name__ == '__main__':
    etl_web_to_gcs_main()