CREATE OR REPLACE EXTERNAL TABLE `platinum-avenue-375715.trips_data_all.external_fhv_tripdata_2019`
OPTIONS (
  format = 'CSV',
  uris = ['gs://hw3_data_lake_platinum-avenue-375715/fhv_tripdata_2019-*.csv.gz']
);

-- Q1

select 
  count(*)
from 
  `trips_data_all.external_fhv_tripdata_2019`;

-- result 43244696

create or replace table `platinum-avenue-375715.trips_data_all.fhv_tripdata_2019_non_partitoned` as
select 
  *
from 
  `trips_data_all.external_fhv_tripdata_2019`; 

--Q2

select 
  count(distinct Affiliated_base_number)
from 
  `trips_data_all.external_fhv_tripdata_2019`;

-- This query will process 0 B when run.


select 
  count(distinct Affiliated_base_number)
from 
  `trips_data_all.fhv_tripdata_2019_non_partitoned`;

-- This query will process 317.94 MB when run.
-- Result 3165

--Q3

select
  count(*)
from
  `trips_data_all.fhv_tripdata_2019_non_partitoned`
where 
  PUlocationID is null and DOlocationID is null;

-- result 717748

-- Q4 Partition by pickup_datetime Cluster on affiliated_base_number

CREATE OR REPLACE TABLE `trips_data_all.fhv_tripdata_2019_partitoned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
SELECT * FROM `trips_data_all.external_fhv_tripdata_2019`;


--Q5
select 
  count(distinct Affiliated_base_number)
from 
  `trips_data_all.fhv_tripdata_2019_non_partitoned`
where DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

-- This query will process 647.87 MB when run.

select 
  count(distinct Affiliated_base_number)
from 
  `trips_data_all.fhv_tripdata_2019_partitoned_clustered`
where DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

-- This query will process 23.05 MB when run.

--Q6 GCP Bucket

--Q7 True

--Q8 look at etl_web_to_gcs-parquet.py 