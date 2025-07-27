package com.dekapx.spark.quickstart;

import com.dekapx.spark.model.Person;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class SparkJavaCollectionToDataframe {
    public static void main(String[] args) {
        SparkSession sparkSession = createSparkSession();
        List<Person> people = createPersons();
        sparkSession.createDataFrame(people, Person.class);

        Dataset<Row> dataFrame = sparkSession.createDataFrame(people, Person.class);
        dataFrame.printSchema();
        dataFrame.show();

        // Group by age and count the number of occurrences
        dataFrame.groupBy("age")
                .count()
                .orderBy("age")
                .show();
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("SparkJavaCollectionToDataframe")
                .master("local[*]")
                .getOrCreate();
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
}
