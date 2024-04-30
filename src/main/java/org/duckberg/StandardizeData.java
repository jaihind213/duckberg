package org.duckberg;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.api.java.UDF1;


import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.HashSet;
import java.util.Set;

/**
 * The raw data we got from the nyc yellow cab data is not consistent with the schema we have in iceberg table.
 * some columns are not in the right data type. example: passenger_count is sometimes int and sometimes long.
 * You might need these vm Options while running on java 17.
 * --illegal-access=permit --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED --add-exports java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED
 * todo: code is a little dirty, but it gets the job done :) dont judge me :P
 */
public class StandardizeData {

    /**
     * This method reads the parquet files from the root data directory, standardizes the schema
     * and writes the files to the output directory.
     * say u have data pulled using the data_pull.sh script into the directory /nyc_yellow_data_staging
     * i.e. u have /nyc_yellow_data_staging/yyyy/mm format
     * then args[0] = /nyc_yellow_data_staging
     * then args[1] = /nyc_yellow_data
     * @param args
     */
    public static void main( String[] args ) {
        String inputDirPath = args[0];
        String outputPath = args[1];
        try (SparkSession spark = SparkSession.builder()
                .appName("iceberg_file_catolog")
                .master("local[*]")
                .config("spark.sql.parquet.timestampNTZ.enabled","false")
                .getOrCreate()) {
            spark.udf().register("intToDouble", intToDouble, DataTypes.DoubleType);
            spark.udf().register("toInt", toInt, DataTypes.IntegerType);
            spark.udf().register("toLong", toLong, DataTypes.LongType);
            spark.conf().set("spark.sql.session.timeZone", "GMT");

            //for(String year: new String[]{"2023","2022","2021","2020","2019","2018", "2017", "2016", "2015", "2014"}){
            for(String year: new String[]{"2014"}){
                //for(String month: new String[]{"01","02","03","04","05","06","07","08","10","11","12", "09"}){
                for(String month: new String[]{"01"}){
                    String filePath = String.format("file://%s/%s/%s/yellow_tripdata_%s-%s.parquet", inputDirPath, year, month,year, month);

                    Dataset<Row> parquetDataset = spark.read().parquet(filePath);
                    parquetDataset.printSchema();
                    parquetDataset = parquetDataset.withColumn("tpep_pickup_datetime", parquetDataset.col("tpep_pickup_datetime").cast("timestamp"));
                    parquetDataset = parquetDataset.withColumn("tpep_dropoff_datetime", parquetDataset.col("tpep_dropoff_datetime").cast("timestamp"));
                    parquetDataset = parquetDataset.withColumn("VendorID",  functions.callUDF("toLong", parquetDataset.col("VendorID")));
                    parquetDataset = parquetDataset.withColumn("PULocationID",  functions.callUDF("toLong", parquetDataset.col("PULocationID")));
                    parquetDataset = parquetDataset.withColumn("DOLocationID",  functions.callUDF("toLong", parquetDataset.col("DOLocationID")));

                    parquetDataset = parquetDataset.withColumn("airport_fee2",  functions.callUDF("toInt", parquetDataset.col("airport_fee")));
                    parquetDataset = parquetDataset.withColumn("passenger_count2",  functions.callUDF("toLong", parquetDataset.col("passenger_count")));
                    parquetDataset = parquetDataset.withColumn("congestion_surcharge2",  functions.callUDF("intToDouble", parquetDataset.col("congestion_surcharge")));
                    parquetDataset = parquetDataset.withColumn("improvement_surcharge2",  functions.callUDF("intToDouble", parquetDataset.col("improvement_surcharge")));
                    parquetDataset = parquetDataset.withColumn("RatecodeID2",  functions.callUDF("toLong", parquetDataset.col("RatecodeID")));
                    parquetDataset = parquetDataset.drop("congestion_surcharge");
                    parquetDataset = parquetDataset.drop("improvement_surcharge");
                    parquetDataset = parquetDataset.drop("airport_fee");
                    parquetDataset = parquetDataset.drop("passenger_count");
                    parquetDataset = parquetDataset.drop("RatecodeID");
                    parquetDataset = parquetDataset.withColumnRenamed("congestion_surcharge2", "congestion_surcharge");
                    parquetDataset = parquetDataset.withColumnRenamed("improvement_surcharge2", "improvement_surcharge");
                    parquetDataset = parquetDataset.withColumnRenamed("airport_fee2", "airport_fee");
                    parquetDataset = parquetDataset.withColumnRenamed("passenger_count2", "passenger_count");
                    parquetDataset = parquetDataset.withColumnRenamed("RatecodeID2", "RatecodeID");

                    parquetDataset.printSchema();
                    String transformedDataPath = "/tmp/" + month;
                    parquetDataset.coalesce(1).write().mode(SaveMode.Overwrite).parquet(transformedDataPath);
                    try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(transformedDataPath ))) {
                        for (Path path : stream) {
                            if (!Files.isDirectory(path) && path.toUri().getPath().endsWith(".parquet")){
                                File newFile = new File(transformedDataPath + "/" + String.format("yellow_tripdata_%s-%s.parquet", year, month));
                                boolean success = path.toFile().renameTo(newFile);
                                if (!success) {
                                    throw new RuntimeException("The file was not renamed. month " + month);
                                }
                                Path sourceFile = Paths.get(newFile.getAbsolutePath()); // File to be moved
                                Path targetFolder = Paths.get(String.format("%s/%s/%s", outputPath, year, month)); // Folder to move the file to
                                FileUtils.forceMkdir(targetFolder.toFile());
                                Path targetFile = targetFolder.resolve(sourceFile.getFileName());

                                // Move the file to the transformedDataPath folder, overwriting if it exists
                                Files.move(sourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
                            }
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

    }

    static UDF1<Object, Double> intToDouble = new UDF1<Object, Double>() {
        public Double call(Object value) throws Exception {
            if(value == null){
                return null;
            }
            //System.out.println(value.getClass());
            if (value instanceof Integer){
                return ((Integer)value).doubleValue();
            }else if (value instanceof Long) {
                return ((Long)value).doubleValue();
            }
            return ((Double)value);

        }
    };

    static UDF1<Object, Integer> toInt = new UDF1<Object, Integer>() {
        public Integer call(Object value) throws Exception {
            if(value == null){
                return null;
            }
            //System.out.println(value.getClass());
            if (value instanceof Double){
                return ((Double)value).intValue();
            }else if (value instanceof Long) {
                return ((Long)value).intValue();
            }
            return (Integer) value;

        }
    };

    static UDF1<Object, Long> toLong = new UDF1<Object, Long>() {
        public Long call(Object value) throws Exception {
            if(value == null){
                return null;
            }
            //System.out.println(value.getClass());
            if (value instanceof Double){
                return ((Double)value).longValue();
            }else if (value instanceof Integer) {
                return ((Integer)value).longValue();
            }
            return (Long) value;

        }
    };

}
