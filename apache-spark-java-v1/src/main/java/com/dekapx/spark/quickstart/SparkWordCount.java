package com.dekapx.spark.quickstart;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class SparkWordCount {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Spark Word Count")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataFrame = readData(sparkSession);
        printData(dataFrame);
        printSchema(dataFrame);
        printModifiedData(dataFrame);
    }

    private static void printData(Dataset<Row> dataFrame) {
        dataFrame.show();
    }

    private static void printSchema(Dataset<Row> dataFrame) {
        dataFrame.printSchema();
    }

    private static void printModifiedData(Dataset<Row> dataFrame) {
        Dataset<Row> modifiedDataFrame = dataFrame
                .withColumn("full_name",
                        concat(col("first_name"),
                                lit(", "),
                                col("last_name")));
        modifiedDataFrame.show();
    }

    private static Dataset<Row> readData(SparkSession sparkSession) {
        return sparkSession
                .read()
                .format("csv")
                .option("header", true)
                .load("src/main/resources/data/word-count.txt");
    }
}
