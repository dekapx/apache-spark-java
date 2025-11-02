package com.dekapx.java.chapter07;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class JavaSimpleTypeCollectionToDataFrame {

    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        List<String> languages = List.of("Java", "Python", "Scala", "JavaScript", "Kotlin");
        Dataset<Row> dataset = spark.createDataset(languages, Encoders.STRING()).toDF();
        dataset.printSchema();
        dataset.show();
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("ApacheSparkQuickStart")
                .master("local[*]")
                .getOrCreate();
    }
}
