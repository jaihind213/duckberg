#!/bin/bash

# Define an array of strings

#years=("2023" "2022" "2021" "2020" "2019")

#years=("2018" "2014" "2015" "2016" "2017")
years=( "2015" "2016" "2017")
months=("01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12")

base_url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
mkdir nyc_yellow_cab_data
project_dir=`pwd`

# Loop through each string in the array
for year in "${years[@]}"
do
	for month in "${months[@]}"; do
		mkdir -p nyc_yellow_cab_data/$year/$month
    		cd  nyc_yellow_cab_data/$year/$month
    		url="${base_url}${year}-${month}.parquet"
    		echo "Downloading $url"
    		wget $url 
    		cd -
                sleep 1
	done
done

cd $project_dir 
