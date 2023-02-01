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
def load_to_bq(path: Path) -> int:
    """Load data to BigQuery"""

    df = pd.read_parquet(path)

    gcp_credentials_block = GcpCredentials.load("dtc-de-user")

    df.to_gbq(destination_table='trips_data_all.rides',
                project_id='platinum-avenue-375715',
                chunksize=500000,
                if_exists='replace',
                credentials=gcp_credentials_block.get_credentials_from_service_account())
    
    return len(df)

@flow()
def etl_gcs_to_bq(month: int = 1, year: int = 2019, color: str = 'yellow') -> int:
    """Main ETL flow to load data into BigQuery"""
    
    path = extract_from_gcs(color, year, month)
    return load_to_bq(path)

@flow(log_prints=True)
def etl_gcs_to_bq_main(months: list[int]=[2,3], year: int = 2019, color: str = 'yellow') -> None:
    """Main ETL flow to load data into BigQuery"""
    
    totalcount = 0

    for month in months:
        totalcount += etl_gcs_to_bq(month, year, color)

    print(totalcount)


if __name__=='__main__':
    etl_gcs_to_bq_main()

