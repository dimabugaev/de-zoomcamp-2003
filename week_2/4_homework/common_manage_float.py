from etl_web_to_gcs_param import etl_web_to_gcs
from etl_gcs_to_bq_param import etl_gcs_to_bq_main
from prefect import flow, task

@flow()
def run_depended_flows(months: list[int]=[2,3], year: int = 2019, color: str = 'yellow', date_column_ptefix: str = 'tpep'):
    for month in months:
        etl_web_to_gcs(month, year, color, date_column_ptefix)
    etl_gcs_to_bq_main(months, year, color)


    
if __name__ == '__main__':
    run_depended_flows()