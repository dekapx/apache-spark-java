package com.dekapx.spark.quickstart;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkWordCount {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Spark Word Count")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataFrame = readData(sparkSession);
        dataFrame.show();
    }

    private static Dataset<Row> readData(SparkSession sparkSession) {
        return sparkSession
                .read()
                .format("csv")
                .option("header", true)
                .load("src/main/resources/data/word-count.txt");
    }
}
