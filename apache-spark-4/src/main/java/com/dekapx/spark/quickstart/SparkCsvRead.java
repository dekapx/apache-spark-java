package com.dekapx.spark.quickstart;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class SparkCsvRead {
    public static final String CSV_FILE_FORMAT = "csv";
    public static final String CSV_FILE_PATH = "src/main/resources/data/movies-data.txt";
    public static final String FILE_HEADER = "header";

    public static void main(String[] args) {
        SparkSession sparkSession = createSparkSession();
        Dataset<Row> dataFrame = buildDataFrame(sparkSession);
        dataFrame.printSchema();
        dataFrame.show();
        applyFilterAndShow(dataFrame);
        printModifiedData(dataFrame);
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("SparkCsvRead")
                .master("local[*]")
                .getOrCreate();
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
                .filter(col("movie_title").contains("Dragon"))
                .select("first_name", "last_name", "movie_title");
        filteredDataFrame.show(3, 45);  // show 3 rows with 45 characters
    }

    private static Dataset<Row> buildDataFrame(SparkSession sparkSession) {
        return sparkSession
                .read()
                .format(CSV_FILE_FORMAT)
                .option(FILE_HEADER, true)
                .load(CSV_FILE_PATH);
    }
}
