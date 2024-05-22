taxis_iceberg_path = ".warehouse/db/nyc_yellow_city_data"
zone_csv = "./taxi_zone_lookup.csv"
query_map = {
    "query_1": f"select count(*) from iceberg_scan('{taxis_iceberg_path}', allow_moved_paths = true) nyc_yellow_city_data;",
    "query_2": f"select count(*),sum(total_amount),extract('year' FROM  tpep_pickup_datetime) as year, extract('month' FROM  tpep_pickup_datetime) as month from iceberg_scan('{taxis_iceberg_path}', allow_moved_paths = true) nyc_yellow_city_data group by year,month;",
    "query_3": f"select count(*),sum(total_amount),extract('year' FROM  tpep_pickup_datetime) as year from iceberg_scan('{taxis_iceberg_path}', allow_moved_paths = true) nyc_yellow_city_data group by year;",
    "query_4": f"select count(*),vendorid,extract('year' FROM  tpep_pickup_datetime) as year, extract('month' FROM  tpep_pickup_datetime) as month from iceberg_scan('{taxis_iceberg_path}', allow_moved_paths = true) nyc_yellow_city_data group by year,month,vendorid;",
    "query_5": f"select count(*),vendorid,extract('year' FROM  tpep_pickup_datetime) as year  from iceberg_scan('{taxis_iceberg_path}', allow_moved_paths = true) nyc_yellow_city_data group by year,vendorid;",
    "query_6": f"select count(*) as total_rides,PULocationID,extract('year' FROM  tpep_pickup_datetime) as year, extract('month' FROM  tpep_pickup_datetime) as month from iceberg_scan('{taxis_iceberg_path}', allow_moved_paths = true) nyc_yellow_city_data group by year,month,PULocationID",
    "query_7": f"select count(*) as total_rides,PULocationID from iceberg_scan('{taxis_iceberg_path}', allow_moved_paths = true) nyc_yellow_city_data group by PULocationID order by total_rides limit 10;",

    "query_8": f"select count(*) as total_rides,DOLocationID,extract('year' FROM  tpep_pickup_datetime) as year, extract('month' FROM  tpep_pickup_datetime) as month from iceberg_scan('{taxis_iceberg_path}', allow_moved_paths = true) nyc_yellow_city_data group by year,month,DOLocationID",
    "query_9": f"select count(*) as total_rides,DOLocationID from iceberg_scan('{taxis_iceberg_path}', allow_moved_paths = true) nyc_yellow_city_data group by DOLocationID order by total_rides limit 10;",

    "query_10": f"SELECT EXTRACT('year' FROM tpep_pickup_datetime) AS year, EXTRACT('month' FROM tpep_pickup_datetime) AS month, AVG(trip_distance) AS avg_trip_distance FROM iceberg_scan('{taxis_iceberg_path}', allow_moved_paths = true) nyc_yellow_city_data  GROUP BY year, month ORDER BY year, month;",
    "query_11": f"SELECT EXTRACT('year' FROM tpep_pickup_datetime) AS year, AVG(trip_distance) AS avg_trip_distance FROM iceberg_scan('{taxis_iceberg_path}', allow_moved_paths = true) nyc_yellow_city_data  GROUP BY year  ORDER BY year;",

    "query_12": f"SELECT passenger_count, avg(total_amount) FROM iceberg_scan('{taxis_iceberg_path}', allow_moved_paths = true) nyc_yellow_city_data GROUP BY passenger_count",
    "query_13": f"SELECT passenger_count, EXTRACT('year' FROM tpep_pickup_datetime) AS year, count(*) FROM iceberg_scan('{taxis_iceberg_path}', allow_moved_paths = true) nyc_yellow_city_data GROUP BY passenger_count, year",
    "query_14": f"SELECT passenger_count, EXTRACT('year' FROM tpep_pickup_datetime) AS year, ceil(trip_distance) AS distance, count(*) FROM iceberg_scan('{taxis_iceberg_path}', allow_moved_paths = true) nyc_yellow_city_data GROUP BY passenger_count, year, distance ORDER BY year, count(*) DESC",
    #query 15 is rewritten because of bug https://github.com/duckdb/duckdb_iceberg/issues/44
    "query_15": f"WITH scan_data AS ( SELECT * FROM iceberg_scan('{taxis_iceberg_path}', allow_moved_paths = true) ), zone_data AS ( SELECT * FROM read_csv('{zone_csv}') ) SELECT tz.zone AS zone, count(*) AS c FROM scan_data td LEFT JOIN zone_data tz ON td.PULocationID = tz.locationid GROUP BY tz.zone ORDER BY c DESC;",
    "query_15_original": f"SELECT tz.zone AS zone, count(*) AS c FROM iceberg_scan('{taxis_iceberg_path}', allow_moved_paths = true) td LEFT JOIN read_csv('{zone_csv}') tz ON td.PULocationID = tz.locationid GROUP BY zone ORDER BY c DESC",
    "query_16": f"select period, count(*) as num_rides, round(avg(trip_duration), 2) as avg_trip_duration, round(avg(trip_distance), 2) as avg_trip_distance, round(sum(trip_distance), 2) as total_trip_distance, round(avg(total_amount), 2) as avg_trip_price, round(sum(total_amount), 2) as total_trip_price, round(avg(tip_amount), 2) as avg_tip_amount from ( select date_part('year', tpep_pickup_datetime) as trip_year, strftime(tpep_pickup_datetime, '%Y-%m') as period, epoch(tpep_dropoff_datetime - tpep_pickup_datetime) as trip_duration, trip_distance, total_amount, tip_amount from iceberg_scan('{taxis_iceberg_path}', allow_moved_paths = true) where trip_year >= 2014 and trip_year < 2024 ) group by period order by period"
}
