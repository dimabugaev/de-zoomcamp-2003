
YEAR="2021"

URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"

#https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz

for TAXI_TYPE in {'green','yellow'}; do
    
    for MONTH in {1..7}; do

        FMONTH=`printf  "%02d" ${MONTH}`
        LOCAL_PREFIX=data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}
        LOCAL_FILENAME=${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz

        URL=${URL_PREFIX}${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz
        mkdir -p $LOCAL_PREFIX

        echo "downloading ${URL}..."
        wget ${URL} -O ${LOCAL_PREFIX}/${LOCAL_FILENAME}
    done
done

