package com.dekapx.spark.quickstart;

import com.dekapx.spark.model.Student;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class SparkJavaObjectToDataFrame {
    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        Dataset<Student> studentDataset = toDataset(spark, createStudents());
        studentDataset.printSchema();
        studentDataset.show();

        Dataset<Row> studentDataFrame = toDataFrame(spark, createStudents());
        studentDataFrame.printSchema();
        studentDataFrame.show();
    }

    private static Dataset<Row> toDataFrame(SparkSession spark, List<Student> students) {
        return spark.createDataFrame(students, Student.class);
    }

    private static Dataset<Student> toDataset(SparkSession spark, List<Student> students) {
        return spark.createDataset(students, Encoders.bean(Student.class));
    }

    private static List<Student> createStudents() {
        return Arrays.asList(
                new Student(1, "John", "Doe", 85.5),
                new Student(2, "Jane", "Smith", 92.0),
                new Student(3, "Mike", "Johnson", 78.0),
                new Student(4, "Sara", "Williams", 88.0),
                new Student(5, "Tom", "Brown", 95.0)
        );
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("PojoDataSet")
                .master("local[*]")
                .getOrCreate();
    }
}
