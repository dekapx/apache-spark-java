package com.dekapx.java.chapter05;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JsonToDataframeToViewWithSqlQuery {
    public static void main(String[] args) {
        SparkSession spark = createSparkSession();

        Dataset<Row> dataFrame = readJsonFile(spark);
        dataFrame.createOrReplaceTempView("students");

        Dataset<Row> result = spark.sql("SELECT firstName, lastName, age, major, gpa FROM students " +
                "WHERE age > 20 AND gpa > 3.0 ORDER BY firstName ASC");

        result.show();
        spark.stop();
    }

    private static Dataset<Row> readJsonFile(SparkSession spark) {
        String filePath = "src/main/resources/chapter04/student-data.json";
        return spark.read()
                .option("multiline", "true")
                .json(filePath);
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("ApacheSparkQuickStart")
                .master("local[*]")
                .getOrCreate();
    }
}
