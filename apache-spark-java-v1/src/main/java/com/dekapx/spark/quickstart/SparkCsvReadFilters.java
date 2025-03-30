package com.dekapx.spark.quickstart;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/**
 * vm-args: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 */
public class SparkCsvReadFilters {
    public static final String CSV_FILE_FORMAT = "csv";
    public static final String CSV_FILE_PATH = "src/main/resources/data/students-data01.txt";
    public static final String FILE_HEADER = "header";

    public static void main(String[] args) {
        SparkSession sparkSession = getSparkSession();
        Dataset<Row> dataFrame = readData(sparkSession);
        printSchema(dataFrame);
        printData(dataFrame);
        applyFiltersAndShow(dataFrame);
    }

    private static SparkSession getSparkSession() {
        return SparkSession
                .builder()
                .appName("Spark CSV Read Filters")
                .master("local[*]")
                .getOrCreate();
    }

    private static Dataset<Row> readData(SparkSession sparkSession) {
        return sparkSession
                .read()
                .format(CSV_FILE_FORMAT)
                .option(FILE_HEADER, true)
                .load(CSV_FILE_PATH);
    }

    private static void printData(Dataset<Row> dataFrame) {
        dataFrame.show();
    }

    private static void printSchema(Dataset<Row> dataFrame) {
        dataFrame.printSchema();
    }

    private static void applyFiltersAndShow(Dataset<Row> dataFrame) {
        Dataset<Row> filteredDataFrame = dataFrame
                .filter(col("marks").gt(70))
                .select("last_name", "first_name", "marks", "city")
                .limit(5)
                .sort("last_name")
                .orderBy("last_name");

        filteredDataFrame.show();
    }
}
