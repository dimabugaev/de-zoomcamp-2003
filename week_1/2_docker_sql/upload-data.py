import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import argparse
from time import time
import os

def main(params):

    tempfile = 'temp_sourse.parquet'

    engine = create_engine(f'postgresql://{params.user}:{params.password}@{params.host}:{params.port}/{params.base}')
    engine.connect()


    os.system(f'wget {params.url} -O {tempfile}')
    
    if params.format.upper() == 'CSV':
        trips = pd.read_csv(tempfile)
    else:
        trips = pq.read_table(tempfile)
        trips = trips.to_pandas()

    


    #print(pd.io.sql.get_schema(trips, params.table, con=engine))

    t_start = time()
    print('loading is begin...')
    trips.to_sql(name=params.table,con=engine,if_exists='replace', chunksize=100000)
    t_finish = time()

    os.system(f'rm -rf {tempfile}')
    print(f'loading is finised in {t_finish-t_start:.3f}')


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host name for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--base', help='database name for postgres')
    parser.add_argument('--table', help='the name of table where we will write the results to')
    parser.add_argument('--url', help='url of source file')
    parser.add_argument('--format', help='"PARQUET" or "CSV" by defoult = parquet', default='PARQUET')

    args = parser.parse_args()
    main(args)

