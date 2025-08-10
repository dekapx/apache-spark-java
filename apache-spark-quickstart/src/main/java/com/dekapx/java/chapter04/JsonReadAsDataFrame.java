package com.dekapx.java.chapter04;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class JsonReadAsDataFrame {
    public static void main(String[] args) {
        SparkSession spark = createSparkSession();

        Dataset<Row> dataFrame = readJsonFile(spark);
        dataFrame.printSchema();
        dataFrame.show();

        spark.stop();
    }

    private static Dataset<Row> readJsonFile(SparkSession spark) {
        String filePath = "src/main/resources/chapter04/student-data.json";
        return spark.read()
                .option("multiline", "true")
                .json(filePath)
                .select("firstName", "lastName", "age", "major", "gpa")
                .sort(col("firstName").asc());
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("ApacheSparkQuickStart")
                .master("local[*]")
                .getOrCreate();
    }
}
