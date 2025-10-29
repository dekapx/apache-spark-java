package com.dekapx.databricks.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

public class SampleSparkJob {
    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        List<Row> rows = createRows();
        List<StructField> structFields = getStructFields();

        Dataset<Row> dataframe = spark.createDataFrame(rows, createStructType(structFields));
        dataframe.printSchema();
        dataframe.show();
    }

    private static List<Row> createRows() {
        return Arrays.asList(
                RowFactory.create(1, "John", "Doe", "john.doe@hotmail.com"),
                RowFactory.create(2, "Jane", "Doe", "jane.doe@gmail.com"),
                RowFactory.create(3, "Mike", "Smith", "mike.smith@yahoo.com"),
                RowFactory.create(4, "Emily", "Jones", "emily.jones@aol.com"),
                RowFactory.create(5, "David", "Brown", "david.brown@outlook.com")
        );
    }

    private static List<StructField> getStructFields() {
        return Arrays.asList(
                createStructField("id", IntegerType, true),
                createStructField("firstName", StringType, true),
                createStructField("lastName", StringType, true),
                createStructField("email", StringType, true)
        );
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("SampleSparkJob")
                .master("local[*]")
                .getOrCreate();
    }
}