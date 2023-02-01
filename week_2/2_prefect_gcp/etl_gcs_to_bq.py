from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task()
def extract_from_gcs(color: str, year: int, mount: int) -> Path:
    """Extract data file to local directory"""
    gcs_path = f"{color}_tripdata_{year}-{mount:02}.parquet"
    gcs_block = GcsBucket.load("gcsbucket")
    gcs_block.get_directory(from_path=gcs_path, local_path=f'data/{color}/')
    return Path(f'data/{color}/{gcs_path}')


@task()
def transform(path: Path)-> pd.DataFrame:
    """Transform data cleaning"""
    df = pd.read_parquet(path)
    print(f"pre: Missing passenger count {df['passenger_count'].isnull().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"pre: Missing passenger count {df['passenger_count'].isnull().sum()}")
    return df


@task()
def load_to_bq(df: pd.DataFrame) -> None:
    """Load data to BigQuery"""


    gcp_credentials_block = GcpCredentials.load("dtc-de-user")

    df.to_gbq(destination_table='trips_data_all.rides',
                project_id='platinum-avenue-375715',
                chunksize=500000,
                if_exists='replace',
                credentials=gcp_credentials_block.get_credentials_from_service_account())


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into BigQuery"""
    color = 'yellow'
    year = 2021
    month = 1
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    load_to_bq(df)



if __name__=='__main__':
    etl_gcs_to_bq()

