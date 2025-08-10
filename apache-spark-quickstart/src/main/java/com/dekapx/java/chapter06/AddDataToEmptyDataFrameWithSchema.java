package com.dekapx.java.chapter06;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.spark.sql.RowFactory.create;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

public class AddDataToEmptyDataFrameWithSchema {
    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        // Create an empty DataFrame
        var emptyDataFrame = spark.createDataFrame(new ArrayList<>(), createStructType(new StructField[]{
                createStructField("id", IntegerType, false),
                createStructField("name", StringType, true)
        }));
        // Show the empty DataFrame
        emptyDataFrame.show();

        // add values to the DataFrame
        var dataWithValues = emptyDataFrame
                .union(spark.createDataFrame(Arrays.asList(
                        create(1, "Alice"),
                        create(2, "Bob"),
                        create(3, "Charlie")
                ), emptyDataFrame.schema()));
        dataWithValues.show();
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("ApacheSparkQuickStart")
                .master("local[*]")
                .getOrCreate();
    }
}
