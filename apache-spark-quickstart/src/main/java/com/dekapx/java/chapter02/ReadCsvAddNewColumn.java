package com.dekapx.java.chapter02;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class ReadCsvAddNewColumn {
    public static void main(String[] args) {
        SparkSession spark = createSparkSession();

        Dataset<Row> dataset = readCsvFile(spark);
        dataset.printSchema();
        dataset.show(5, false); // Show 5 rows without truncating

        // Add a new column 'full_name' by concatenating 'first_name' and 'last_name'
        dataset = dataset
                .withColumn("full_name",
                        concat(
                                col("first_name"),
                                lit(", "),
                                col("last_name")));
        dataset.printSchema();
        dataset.show();

        // Drop the 'first_name' and 'last_name' columns
        dataset = dataset.drop("first_name", "last_name");
        dataset.printSchema();
        dataset.show();

        spark.stop();
    }

    private static Dataset<Row> readCsvFile(SparkSession spark) {
        String filePath = "src/main/resources/chapter02/data.csv";
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(filePath);
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("ApacheSparkQuickStart")
                .master("local[*]")
                .getOrCreate();
    }
}
