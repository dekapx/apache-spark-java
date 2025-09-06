package com.dekapx.java.chapter07;

import com.dekapx.java.chapter07.model.Person;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class JavaCollectionToDataFrameWithEncoders {
    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        List<Person> people = createPersons();
        Encoder<Person> personEncoder = Encoders.bean(Person.class);

        // Create Dataset using Encoders
        Dataset<Person> personDataset = spark.createDataset(people, personEncoder);
        personDataset.printSchema();
        personDataset.show();

        // Convert Dataset to DataFrame
        Dataset<Row> dataFrame = personDataset.toDF();
        dataFrame.printSchema();
        dataFrame.show();

        // Group by age and count the number of occurrences
        dataFrame.groupBy("age")
                .count()
                .orderBy("age")
                .show();
    }

    private static List<Person> createPersons() {
        return List.of(
                new Person("John", 30),
                new Person("Jane", 25),
                new Person("Mike", 45),
                new Person("Sara", 28),
                new Person("Tom", 30),
                new Person("Anna", 22),
                new Person("Bob", 45),
                new Person("Alice", 22)
        );
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("ApacheSparkQuickStart")
                .master("local[*]")
                .getOrCreate();
    }
}

