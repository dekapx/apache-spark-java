package com.dekapx.spark.quickstart;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class SparkCsvToDataframeAsView {
    private static final String CSV_FILE_FORMAT = "csv";
    private static final String CSV_FILE_HEADER = "header";
    private static final String CSV_FILE_PATH = "src/main/resources/data/sample-trades.csv";

    public static void main(String[] args) {
        SparkCsvToDataframeAsView job = new SparkCsvToDataframeAsView();
        job.trigger();
    }

    private void trigger() {
        log.info("Starting Spark CSV to Parquet job...");
        SparkSession sparkSession = createSparkSession();
        Dataset<Row> dataFrame = csvToDataFrame(sparkSession);
        dataFrame.createOrReplaceTempView("GLOBAL_STOCK");
        dataFrame.show();
        Dataset<Row> buyStockSummary = sparkSession.sql("SELECT * FROM GLOBAL_STOCK WHERE type = 'BUY'");
        buyStockSummary.show();

        Dataset<Row> sellStockSummary = sparkSession.sql("SELECT * FROM GLOBAL_STOCK WHERE type = 'SELL'");
        sellStockSummary.show();
    }

    private SparkSession createSparkSession() {
        log.info("Creating Spark session...");
        return SparkSession
                .builder()
                .appName("SparkCsvToParquetJob")
                .master("local[*]")
                .getOrCreate();
    }

    private Dataset<Row> csvToDataFrame(SparkSession sparkSession) {
        log.info("Reading CSV file into DataFrame...");
        return sparkSession
                .read()
                .format(CSV_FILE_FORMAT)
                .option(CSV_FILE_HEADER, true)
                .load(CSV_FILE_PATH);
    }
}
