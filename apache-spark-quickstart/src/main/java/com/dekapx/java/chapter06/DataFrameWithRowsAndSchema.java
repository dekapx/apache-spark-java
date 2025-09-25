package com.dekapx.java.chapter06;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.util.Arrays;
import java.util.List;

import static java.sql.Date.valueOf;

public class DataFrameWithRowsAndSchema {
    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        List<Row> rows = createRows();
        List<StructField> structFields = getStructFields();

        Dataset<Row> dataframe = spark.createDataFrame(rows, DataTypes.createStructType(structFields));
        dataframe.printSchema();
        dataframe.show();
    }

    private static List<Row> createRows() {
        return Arrays.asList(
                RowFactory.create(1, "John", "Doe", "john.doe@hotmail.com", valueOf("1990-01-01")),
                RowFactory.create(2, "Jane", "Doe", "jane.doe@gmail.com", valueOf("1992-02-02")),
                RowFactory.create(3, "Mike", "Smith", "mike.smith@yahoo.com", valueOf("1985-03-03")),
                RowFactory.create(4, "Emily", "Jones", "emily.jones@aol.com", valueOf("1995-04-04")),
                RowFactory.create(5, "David", "Brown", "david.brown@outlook.com", valueOf("1988-05-05"))
        );
    }

    private static List<StructField> getStructFields() {
        return Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("firstName", DataTypes.StringType, true),
                DataTypes.createStructField("lastName", DataTypes.StringType, true),
                DataTypes.createStructField("email", DataTypes.StringType, true),
                DataTypes.createStructField("dateOfBirth", DataTypes.DateType, true)
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
