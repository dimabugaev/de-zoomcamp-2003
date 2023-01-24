--Question 3. Count records 2019-01-15

select 
	COUNT (*)
from green_taxi_data
where lpep_pickup_datetime::DATE = to_date('20190115', 'YYYYMMDD')
	and lpep_dropoff_datetime::DATE = to_date('20190115', 'YYYYMMDD');

--Question 4. Largest trip for each day

select
	lpep_pickup_datetime::DATE data_trip,
	trip_distance
from green_taxi_data
order by
	trip_distance desc	
limit 1;

--Question 5. The number of passengers 2019-01-15

select
	passenger_count,
	COUNT (*)
from green_taxi_data
where
	passenger_count IN(2,3) 
	AND lpep_pickup_datetime::DATE = to_date('20190101', 'YYYYMMDD')
group by
	passenger_count;

--Question 6. Largest tip in the Astoria Zone

select
	pickup."Zone" pickup_zone,
	dropp."Zone" drop_zone,
	trips.tip_amount tip_amount
from green_taxi_data trips 
	left join zones pickup on trips."PULocationID" = pickup."LocationID"
	left join zones dropp on trips."DOLocationID" = dropp."LocationID"
where
	pickup."Zone" = 'Astoria'
order by tip_amount desc	
limit 1