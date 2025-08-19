package com.dekapx.java.chapter09;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFrameRemoveDuplicates {
    public static void main(String[] args) {
        SparkSession spark = createSparkSession();

        Dataset<Row> dataset = readCsvFile(spark);
        dataset.show();
        System.out.println("Total records: " + dataset.count());

        // Remove duplicates based on student_id column
        Dataset<Row> distinctDataset = dataset.dropDuplicates("student_id");
        distinctDataset.show();
        System.out.println("Total records: " + distinctDataset.count());
        spark.stop();
    }

    private static Dataset<Row> readCsvFile(SparkSession spark) {
        String filePath = "src/main/resources/chapter09/student-data.csv";
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
