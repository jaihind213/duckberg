package org.duckberg;

import org.apache.spark.sql.*;

/**
 * spark with iceberg Hadoop catalog pointing to warehouse on local file system.
 * You might need these vm Options while running on java 17.
 * --illegal-access=permit --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED --add-exports java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED
 * @param args
 * args[0] = path to parquet data dir i.e. ./nyc_yellow_cab_data
 * args[1] = path to parquet data dir i.e ./taxi_zone_lookup.csv
 */
public class LoadIceberg {

    public static String catalogName = "local";
    public static String warehouseDir = ".warehouse";//in project dir

    public static String tripsTableName = String.format("%s.db.nyc_yellow_city_data", catalogName);
    public static String zoneLookupTableName = String.format("%s.db.zone_lookup", catalogName);
    public static String createTripsTableSql = String.format("CREATE TABLE %s ( VendorID BIGINT, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime timestamp, passenger_count BIGINT, trip_distance DOUBLE, RatecodeID BIGINT, store_and_fwd_flag STRING, PULocationID BIGINT, DOLocationID BIGINT, payment_type BIGINT, fare_amount DOUBLE, extra DOUBLE, mta_tax DOUBLE, tip_amount DOUBLE, tolls_amount DOUBLE, improvement_surcharge DOUBLE, total_amount DOUBLE, congestion_surcharge DOUBLE, airport_fee INT ) USING iceberg", tripsTableName);
    public static String createZoneTableSql = String.format("CREATE TABLE %s ( LocationID INT, Borough STRING, Zone STRING, ServiceZone STRING ) USING iceberg", zoneLookupTableName);

    public static void main(String[] args) {
        String dataDirPath = args[0];
        String zoneLookCsvPath = args[1];
        System.out.println("reading data from dir: " + dataDirPath);

        try (SparkSession spark = SparkSession.builder()
                .appName("iceberg_file_catalog")
                .master("local[*]")
                //spark.sql.catalog.<catalog_name> = is the namespace
                .config("spark.sql.parquet.timestampNTZ.enabled", "false")
                .config(String.format("spark.sql.catalog.%s", catalogName), "org.apache.iceberg.spark.SparkCatalog")
                .config(String.format("spark.sql.catalog.%s.type", catalogName), "hadoop")
                //.config(String.format("spark.sql.catalog.%s.catalog-impl", catalogName),"class of custom impl like say gprc etc")
                .config(String.format("spark.sql.catalog.%s.warehouse", catalogName), warehouseDir)
                //--conf spark.sql.catalog.my_iceberg_catalog.uri=$URI -->for jdbc/hive/nessie/rest
                .getOrCreate()) {
            // mandatory: u have to specify warehouse for every catalog(rest/jdbc etc) if u want to have iceberg tables. it can be s3
            ///
            spark.conf().set("spark.sql.session.timeZone", "GMT");
            long start = System.currentTimeMillis();
            spark.sql(createTripsTableSql);
            spark.sql(createZoneTableSql);
            Dataset<Row> parquetDataset = spark.read().parquet(dataDirPath);
            parquetDataset.writeTo(tripsTableName).createOrReplace();
            Dataset<Row> zoneDataset = spark.read().option("header", "true").csv(zoneLookCsvPath);
            zoneDataset.writeTo(zoneLookupTableName).createOrReplace();
            long end = System.currentTimeMillis();
            long loadingTimeTakenMs = (end - start);
            System.out.println("LOADING OVER in...." + loadingTimeTakenMs);
        }
        //LOADING OVER in....301997
    }
}
