package com.dekapx.spark.quickstart;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;

/**
 * vm-args: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 */
public class StringDataSetExample {
    public static void main(String[] args) {
        String[] data = {"Java", "Python", "Scala", "C++", "JavaScript"};
        SparkSession sparkSession = createSparkSession();
        Dataset<String> dataset = sparkSession.createDataset(Arrays.asList(data), Encoders.STRING());
        dataset.printSchema();
        dataset.show();
        dataset.orderBy(col("value")).show();
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("StringDataSet")
                .master("local[*]")
                .getOrCreate();
    }
}
