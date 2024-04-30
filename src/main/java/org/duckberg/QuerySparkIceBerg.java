package org.duckberg;

import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;

import static org.duckberg.LoadIceberg.tripsTableName;
import static org.duckberg.LoadIceberg.zoneLookupTableName;

/**
 * spark with iceberg Hadoop catalog pointing to warehouse on local file system.
 * You might need these vm Options while running on java 17.
 * --illegal-access=permit --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED --add-exports java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED
 * @param args
 * args[0] = query_id , for query_id refer to the static map in the class.
 */
public class QuerySparkIceBerg {
    public static String catalogName = "local";
    public static String warehouseDir = ".warehouse";//in project dir

    static HashMap<String,String> queryMap = new HashMap<>();

    static {
        queryMap.put("query_1", String.format("select count(*) from %s", tripsTableName));
        queryMap.put("query_2", String.format("select count(*),sum(total_amount),extract(year FROM  tpep_pickup_datetime) as year, extract(month FROM  tpep_pickup_datetime) as month from %s group by year,month;", tripsTableName));
        queryMap.put("query_3", String.format("select count(*),sum(total_amount),extract(year FROM  tpep_pickup_datetime) as year from %s group by year;", tripsTableName));
        queryMap.put("query_4", String.format("select count(*),vendorid,extract(year FROM  tpep_pickup_datetime) as year, extract(month FROM  tpep_pickup_datetime) as month from %s group by year,month,vendorid;", tripsTableName));
        queryMap.put("query_5", String.format("select count(*),vendorid,extract(year FROM  tpep_pickup_datetime) as year  from %s group by year,vendorid;", tripsTableName));
        queryMap.put("query_6", String.format("select count(*) as total_rides,PULocationID,extract(year FROM  tpep_pickup_datetime) as year, extract(month FROM  tpep_pickup_datetime) as month from %s group by year,month,PULocationID", tripsTableName));
        queryMap.put("query_7", String.format("select count(*) as total_rides,PULocationID from %s group by PULocationID order by total_rides limit 10;", tripsTableName));
        queryMap.put("query_8", String.format("select count(*) as total_rides,DOLocationID,extract(year FROM  tpep_pickup_datetime) as year, extract(month FROM  tpep_pickup_datetime) as month from %s group by year,month,DOLocationID", tripsTableName));
        queryMap.put("query_9", String.format("select count(*) as total_rides,DOLocationID from %s group by DOLocationID order by total_rides limit 10;", tripsTableName));
        queryMap.put("query_10", String.format("SELECT EXTRACT(year FROM tpep_pickup_datetime) AS year, EXTRACT(month FROM tpep_pickup_datetime) AS month, AVG(trip_distance) AS avg_trip_distance FROM %s  GROUP BY year, month ORDER BY year, month;", tripsTableName));
        queryMap.put("query_11", String.format("SELECT EXTRACT(year FROM tpep_pickup_datetime) AS year, AVG(trip_distance) AS avg_trip_distance FROM %s  GROUP BY year  ORDER BY year;", tripsTableName));
        queryMap.put("query_12", String.format("SELECT passenger_count, avg(total_amount) FROM %s GROUP BY passenger_count", tripsTableName));
        queryMap.put("query_13", String.format("SELECT passenger_count, EXTRACT(year FROM tpep_pickup_datetime) AS year, count(*) FROM %s GROUP BY passenger_count, year", tripsTableName));
        queryMap.put("query_14", String.format("SELECT passenger_count, EXTRACT(year FROM tpep_pickup_datetime) AS year, ceil(trip_distance) AS distance, count(*) FROM %s GROUP BY passenger_count, year, distance ORDER BY year, count(*) DESC", tripsTableName));
        queryMap.put("query_15", String.format("SELECT tz.zone AS zone, count(*) AS c FROM %s td LEFT JOIN %s tz ON td.PULocationID = tz.locationid GROUP BY zone ORDER BY c DESC", tripsTableName, zoneLookupTableName));
        queryMap.put("query_16", String.format("select period, count(*) as num_rides, round(avg(trip_duration), 2) as avg_trip_duration, round(avg(trip_distance), 2) as avg_trip_distance, round(sum(trip_distance), 2) as total_trip_distance, round(avg(total_amount), 2) as avg_trip_price, round(sum(total_amount), 2) as total_trip_price, round(avg(tip_amount), 2) as avg_tip_amount from ( select extract(year FROM tpep_pickup_datetime) as trip_year, date_format(tpep_pickup_datetime, 'yyyy-MM') as period, unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime) as trip_duration, trip_distance, total_amount, tip_amount from %s where extract(year FROM tpep_pickup_datetime) >= 2014 and extract(year FROM tpep_pickup_datetime) < 2024 ) group by period order by period", tripsTableName));
        //queryMap.put("query_17", String.format("select extract(year FROM tpep_pickup_datetime) as trip_year from %s where trip_year >= 2014 and trip_year < 2024 limit 1", tripsTableName));
    }

    public static void main( String[] args ) throws Exception{
        String queryId = args[0];
        System.out.println("iceberg table name: " + tripsTableName);
        String sql = queryMap.get(queryId);
        System.out.println("sql: " + sql);

        try (SparkSession spark = SparkSession.builder()
                .appName("iceberg_file_catolog")
                .master("local[*]")
                .config("spark.sql.parquet.timestampNTZ.enabled","false")
                .config(String.format("spark.sql.catalog.%s", catalogName), "org.apache.iceberg.spark.SparkCatalog")
                .config(String.format("spark.sql.catalog.%s.type", catalogName), "hadoop")
                .config(String.format("spark.sql.catalog.%s.warehouse", catalogName), warehouseDir)
                .getOrCreate()) {

            spark.conf().set("spark.sql.session.timeZone", "GMT");
            long start = System.currentTimeMillis();
            spark.sql(sql).write().option("numRows",Integer.parseInt(System.getProperty("NUM_ROWS_SHOW", "50000"))).format("console").save();
            long end = System.currentTimeMillis();
            long queryTimeTakenMs = (end - start);

            String filePath = "results_iceberg.csv";
            // Create a File object
            File file = new File(filePath);
            if (!file.exists()) {
                // If the file doesn't exist, create it and write default content
                FileWriter writer = new FileWriter(file);
                writer.write("query_id,engine,query_time_ms");
                writer.close();
            }
            FileWriter writer = new FileWriter(file, true);
            writer.write(String.format("\n%s,sparkIceberg,%s", queryId, queryTimeTakenMs));
            writer.close();

        }
    }
}
