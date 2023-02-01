from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas dataFrame"""
    return pd.read_csv(dataset_url)


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""

    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    print(df.head(2))
    print(f"collumns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write taxi data locally as parquet file"""
    Path(f'data/{color}').mkdir(parents=True, exist_ok=True)
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression='gzip')
    return path

@task()
def write_to_gcs(path: Path) -> None:
    """Upload data file to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("gcsbucket")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=f"{path}", to_path=path.name)


@flow()
def etl_web_to_gcs(month: int, year: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    print(dataset_file)
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'
    print(dataset_url)

    df = fetch(dataset_url)
    df_cleaned = clean(df)
    path = write_local(df_cleaned, color, dataset_file)
    write_to_gcs(path)

@flow()
def etl_parent_flow(months: list[int]=[1,2], year: int = 2021, color: str = 'yellow'):

    for month in months:
        etl_web_to_gcs(month, year, color)
    

if __name__ == '__main__':
    color = 'yellow'
    year = 2021
    months = [1, 2, 3]
    etl_parent_flow(months, year, color)