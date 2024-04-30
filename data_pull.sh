#!/bin/bash

STAGING_DIR=nyc_yellow_cab_data_staging
base_url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
years=("2023" "2022" "2021" "2020" "2019" "2018" "2017" "2016" "2015" "2014")
months=("01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12")


mkdir $STAGING_DIR
project_dir=`pwd`

# Loop through each string in the array
for year in "${years[@]}"
do
	for month in "${months[@]}"; do
		mkdir -p nyc_yellow_cab_data_staging/$year/$month
    		cd  $STAGING_DIR/$year/$month
    		url="${base_url}${year}-${month}.parquet"
    		echo "Downloading $url"
    		wget $url 
    		cd -
                sleep 1
	done
done

cd $project_dir 
