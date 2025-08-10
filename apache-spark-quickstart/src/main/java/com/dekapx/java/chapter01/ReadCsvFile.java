package com.dekapx.java.chapter01;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadCsvFile {
    public static void main(String[] args) {
        SparkSession spark = createSparkSession();

        Dataset<Row> dataset = readCsvFile(spark);
        dataset.printSchema();
        dataset.show();

        spark.stop();
    }

    private static Dataset<Row> readCsvFile(SparkSession spark) {
        String filePath = "src/main/resources/chapter01/data.csv";
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(filePath);
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("ApacheSparkHelloWorld")
                .master("local[*]")
                .getOrCreate();
    }
}
