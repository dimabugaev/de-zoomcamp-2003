from prefect_sqlalchemy import SqlAlchemyConnector
#with SqlAlchemyConnector.load("pgdatabase") as database_block:

import pandas as pd
import pyarrow.parquet as pq
from time import time
import os
from prefect import flow, task

@task(log_prints=True, retries=3)
def extract_data(url: str, format = 'parquet'):

    tempfile = 'temp_sourse.' + format.lower()

    os.system(f'wget {url} -O {tempfile}')
    
    if format.upper() == 'CSV':
        trips = pd.read_csv(tempfile)
    else:
        trips = pq.read_table(tempfile)
        trips = trips.to_pandas()

    os.system(f'rm -rf {tempfile}')

    return trips

@task(log_prints=True, retries=3)
def transform_data(dataframe_to_transform):
    return dataframe_to_transform[dataframe_to_transform['passenger_count']>0]

@task(log_prints=True, retries=3)
def load_data(dataframe_to_load, table_name='yellow_taxi_data_new'):


    connection_block = SqlAlchemyConnector.load("pgdatabase")
    with connection_block.get_connection(begin=False) as engine:
        t_start = time()
        print('loading is begin...')
        dataframe_to_load.to_sql(name=table_name,con=engine,if_exists='replace', chunksize=100000)
        t_finish = time()
        print(f'loading is finised in {t_finish-t_start:.3f}')

    #os.system(f'rm -rf {tempfile}')
    

@flow(name="Ingest Data")
def main_flow():
    
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2021-01.parquet'
    dataf = extract_data(url)    
    dataf = transform_data(dataf)
    load_data(dataf, 'green_taxi_data_202101')



if __name__ == '__main__':
    main_flow()
    

