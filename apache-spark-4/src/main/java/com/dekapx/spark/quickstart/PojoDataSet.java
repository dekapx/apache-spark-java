package com.dekapx.spark.quickstart;

import com.dekapx.spark.model.Person;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;

/**
 * vm-args: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 */
public class PojoDataSet {
    public static void main(String[] args) {
        SparkSession sparkSession = createSparkSession();
        Dataset<Person> dataset = createDataset(sparkSession);
        dataset.printSchema();
        dataset.show();
        dataset.groupBy(col("age"))
                .count()
                .orderBy(col("age"))
                .show();
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("PojoDataSet")
                .master("local[*]")
                .getOrCreate();
    }

    private static Dataset<Person> createDataset(SparkSession sparkSession) {
        return sparkSession.createDataset(
                Arrays.asList(
                        new Person("John", 30),
                        new Person("Jane", 25),
                        new Person("Mike", 45),
                        new Person("Sara", 28),
                        new Person("Tom", 30),
                        new Person("Anna", 22),
                        new Person("Bob", 45),
                        new Person("Alice", 22)
                ),
                Encoders.bean(Person.class)
        );
    }
}
