#!/bin/bash

base_url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
YEAR=$1
mkdir nyc_data
project_dir=`pwd`

for month in {01..12}; do
    mkdir -p nyc_yellow_cab_data/$YEAR/$month
    cd  nyc_yellow_cab_data/$YEAR/$month
    url="${base_url}${YEAR}-${month}.parquet"
    echo "Downloading $url"
    wget "$url"
    cd -
done
cd $project_dir
