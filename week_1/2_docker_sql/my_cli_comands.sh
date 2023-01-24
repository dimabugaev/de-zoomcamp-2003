version: '1'

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTRGES_DB: airflow
    volumes:
       - postgres-db-volume:/var/lib/posgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    restart: always

docker network create pg-network

docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network pg-network \
  --name pg-database \
  postgres:13


docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 5050:80 \
  --network pg-network \
  --name pg-admin \
  dpage/pgadmin4

URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"

python3 upload-data.py \
    --user root \
    --password root \
    --host pg-database \
    --port 5432 \
    --base ny_taxi \
    --table yellow_taxi_data \
    --url ${URL}

docker build -t taxi_ingest:v001 .

docker run -it \
  --network pg-network \
  taxi_ingest:v001 \
    --user root \
    --password root \
    --host pg-database \
    --port 5432 \
    --base ny_taxi \
    --table yellow_taxi_data \
    --url ${URL}

URL="https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"

docker run -it \
  --network 2_docker_sql_default \
  taxi_ingest:v002 \
    --user root \
    --password root \
    --host pgdatabase \
    --port 5432 \
    --base ny_taxi \
    --table zones \
    --url ${URL} \
    --format csv


URL="https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-01.parquet"

URL=${pwd}/green_tripdata_2019-01.csv 

docker run -it \
  --network 2_docker_sql_default \
  taxi_ingest:v002 \
    --user root \
    --password root \
    --host pgdatabase \
    --port 5432 \
    --base ny_taxi \
    --table green_taxi_data \
    --url ${URL} \
    --format csv  