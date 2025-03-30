package com.dekapx.spark.quickstart;

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

/**
 * vm-args: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 */
public class SparkInferSchema {
    public static final String CSV_FILE_FORMAT = "csv";
    public static final String CSV_FILE_PATH = "src/main/resources/data/students-data02.txt";
    public static final String FILE_HEADER = "header";
    public static final String SEPARATOR_OPTION = "sep";
    public static final String SEPARATOR_VALUE = "|";
    public static final String DATE_FORMAT_OPTION = "dateFormat";
    public static final String DATE_FORMAT = "yyyy-MM-dd";


    public static void main(String[] args) {
        SparkSession sparkSession = createSparkSession();
        StructType schema = defineSchema();
        Dataset<Row> dataFrame = readData(sparkSession, schema);
        dataFrame.printSchema();
        dataFrame.show();
    }

    private static StructType defineSchema() {
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

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("SparkInferSchema")
                .master("local[*]")
                .getOrCreate();
    }

    private static Dataset<Row> readData(SparkSession sparkSession, StructType schema) {
        return sparkSession
                .read()
                .format(CSV_FILE_FORMAT)
                .option(FILE_HEADER, true)
                .option(SEPARATOR_OPTION, SEPARATOR_VALUE)
                .option(DATE_FORMAT_OPTION, DATE_FORMAT)
                .schema(schema)
                .load(CSV_FILE_PATH);
    }
}
