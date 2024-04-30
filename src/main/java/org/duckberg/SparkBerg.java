package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class SparkBerg {

    public static String catalogName = "local";
    public static String warehouseDir = ".warehouse";//in project dir
    public static void main( String[] args ) {
        String createTableSql = args[0];
        String dataPath = args[1];
        String sql = args[2];

        SparkSession spark = SparkSession.builder()
                .appName("iceberg_file_catolog")
                .master("local[*]")
                //spark.sql.catalog.<catalog_name> = is the namespace
                .config(String.format("spark.sql.catalog.%s", catalogName),"org.apache.iceberg.spark.SparkCatalog")
                .config(String.format("spark.sql.catalog.%s.type", catalogName),"hadoop")
                //.config(String.format("spark.sql.catalog.%s.catalog-impl", catalogName),"class of custom impl like say gprc etc")
                .config(String.format("spark.sql.catalog.%s.warehouse", catalogName), warehouseDir)
                //--conf spark.sql.catalog.my_iceberg_catalog.uri=$URI -->for jdbc/hive/nessie/rest
                .getOrCreate();

        // mandatory: u have to specify warehouse for every catalog(rest/jdbc etc) if u want to have iceberg tables. it can be s3
        ///
        spark.conf().set("spark.sql.session.timeZone", "GMT");

        //create table test (id INTEGER, age INTEGER, salary INTEGER, name VARCHAR)
        spark.sql("create table if not exists local.db.test (id int, age int, salary int, name string) USING iceberg");


        long start =System.currentTimeMillis();
        Dataset<Row> parquetDataset = spark.read().parquet("file:///Users/vishnuch/work/gitcode/sample_data/data/*.parquet");
        //System.out.println("---");
        //System.out.println(parquetDataset.count());
        //System.out.println("---");
        //parquetDataset.show(10);
        parquetDataset.writeTo("local.db.test").createOrReplace();
        System.out.println("LOADING OVER...");

        spark.sql("sELECT age ,sum(salary) FROM local.db.test group by age").show(1000, false);
        long end =System.currentTimeMillis();
        System.out.println("===================" + (end-start));

        spark.close();

    }
}
