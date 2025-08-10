package com.dekapx.java.chapter06;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

public class EmptyDataFrameWithSchema {
    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        // Create an empty DataFrame
        var emptyDataFrame = spark.createDataFrame(new ArrayList<>(), createStructType(new StructField[]{
                createStructField("id", IntegerType, false),
                createStructField("name", StringType, true)
        }));
        // Show the empty DataFrame
        emptyDataFrame.show();
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("ApacheSparkQuickStart")
                .master("local[*]")
                .getOrCreate();
    }
}
