package com.dekapx.spark.quickstart;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

/**
 * vm-args: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 */
public class SparkCsvRead {
    public static final String CSV_FILE_FORMAT = "csv";
    public static final String CSV_FILE_PATH = "src/main/resources/data/sample-data.txt";
    public static final String FILE_HEADER = "header";

    public static void main(String[] args) {
        SparkSession sparkSession = getSparkSession();
        Dataset<Row> dataFrame = readData(sparkSession);
        printSchema(dataFrame);
        printData(dataFrame);
        applyFilterAndShow(dataFrame);
        printModifiedData(dataFrame);
    }

    private static SparkSession getSparkSession() {
        return SparkSession
                .builder()
                .appName("Spark Word Count")
                .master("local[*]")
                .getOrCreate();
    }

    private static void printData(Dataset<Row> dataFrame) {
        dataFrame.show();
    }

    private static void printSchema(Dataset<Row> dataFrame) {
        dataFrame.printSchema();
    }

    private static void printModifiedData(Dataset<Row> dataFrame) {
        Dataset<Row> modifiedDataFrame = dataFrame
                .withColumn("full_name",
                        concat(col("first_name"),
                                lit(", "),
                                col("last_name")));
        modifiedDataFrame.show();
    }

    private static void applyFilterAndShow(Dataset<Row> dataFrame) {
        Dataset<Row> filteredDataFrame = dataFrame
                .filter(col("comment").contains("Dragon"))
                .select("first_name", "last_name", "comment");
        filteredDataFrame.show();
    }

    private static Dataset<Row> readData(SparkSession sparkSession) {
        return sparkSession
                .read()
                .format(CSV_FILE_FORMAT)
                .option(FILE_HEADER, true)
                .load(CSV_FILE_PATH);
    }
}
