package com.dekapx.java.chapter06;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

public class WriteDataFrameWithSchemaToCsv {
    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        // Create an empty DataFrame with a schema
        Dataset<Row> emptyDataFrame = spark.createDataFrame(new ArrayList<>(), createStructType(new StructField[]{
                createStructField("id", IntegerType, false),
                createStructField("name", StringType, true)
        }));
        emptyDataFrame.show();

        // add values to the DataFrame
        Dataset<Row> dataWithValues = emptyDataFrame
                .union(spark.createDataFrame(Arrays.asList(
                        RowFactory.create(1, "Alice"),
                        RowFactory.create(2, "Bob"),
                        RowFactory.create(3, "Charlie")
                ), emptyDataFrame.schema()));
        dataWithValues.show();

        String output = "src/main/resources/chapter06/output/person-data.csv";
        dataWithValues
                .coalesce(1)
                .write()
                .option("header", "true")
                .csv(output);
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("ApacheSparkQuickStart")
                .master("local[*]")
                .getOrCreate();
    }
}
