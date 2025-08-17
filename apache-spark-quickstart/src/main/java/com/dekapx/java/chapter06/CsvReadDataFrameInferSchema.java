package com.dekapx.java.chapter06;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

public class CsvReadDataFrameInferSchema {
    private static final String FILE_HEADER = "header";
    private static final String SEPARATOR_OPTION = "sep";
    private static final String SEPARATOR_VALUE = "|";
    private static final String DATE_FORMAT_OPTION = "dateFormat";
    private static final String DATE_FORMAT = "yyyy-MM-dd";

    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        StructType schema = schemaDefinition();

        Dataset<Row> dataset = readCsvFile(spark, schema);
        dataset.printSchema();
        dataset.show();

        spark.stop();
    }

    private static StructType schemaDefinition() {
        StructType schema = createStructType(new StructField[] {
                createStructField("id", IntegerType, false),
                createStructField("first_name", StringType, false),
                createStructField("middle_name", StringType, true),
                createStructField("last_name", StringType, false),
                createStructField("date_of_birth", DateType, false),
                createStructField("marks", DoubleType, false)
        });
        return schema;
    }

    private static Dataset<Row> readCsvFile(SparkSession spark, StructType schema) {
        String filePath = "src/main/resources/chapter06/students-data.csv";
        return spark.read()
                .option(FILE_HEADER, true)
                .option(SEPARATOR_OPTION, SEPARATOR_VALUE)
                .option(DATE_FORMAT_OPTION, DATE_FORMAT)
                .schema(schema)
                .csv(filePath);
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("ApacheSparkQuickStart")
                .master("local[*]")
                .getOrCreate();
    }
}
