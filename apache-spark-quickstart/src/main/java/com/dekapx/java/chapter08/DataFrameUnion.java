package com.dekapx.java.chapter08;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFrameUnion {
    private static final String FILE_PATH_1 = "src/main/resources/chapter08/car-sales-ca.csv";
    private static final String FILE_PATH_2 = "src/main/resources/chapter08/car-sales-ny.csv";

    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        Dataset<Row> datasetCA = readCsvFile(spark, FILE_PATH_1);
        datasetCA.printSchema();
        datasetCA.show();

        Dataset<Row> datasetNY = readCsvFile(spark, FILE_PATH_2);
        datasetNY.printSchema();
        datasetNY.show();

        Dataset<Row> unionDataset = datasetCA.union(datasetNY);
        unionDataset.printSchema();
        unionDataset.show();
    }

    private static Dataset<Row> readCsvFile(SparkSession spark, String filePath) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(filePath)
                .drop("transaction_id");
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("ApacheSparkQuickStart")
                .master("local[*]")
                .getOrCreate();
    }
}
