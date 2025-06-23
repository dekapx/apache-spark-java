package com.dekapx.spark.quickstart;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class SparkCsvToParquetJob {
    private static final String CSV_FILE_FORMAT = "csv";
    private static final String CSV_FILE_HEADER = "header";
    private static final String CSV_FILE_PATH = "src/main/resources/data/sample-trades.csv";
    private static final String PARQUET_FILE_PATH = "src/main/resources/output/sample.parquet";

    public static void main(String[] args) {
        SparkCsvToParquetJob job = new SparkCsvToParquetJob();
        job.trigger();
    }

    public void trigger() {
        log.info("Starting Spark CSV to Parquet job...");
        SparkSession sparkSession = createSparkSession();
        Dataset<Row> dataFrame = csvToDataFrame(sparkSession);
        writeDataFrameToParquet(dataFrame);
        Dataset<Row> parquetDataFrame = sparkSession
                .read()
                .parquet(PARQUET_FILE_PATH);
        parquetDataFrame.show();
        log.info("Spark CSV to Parquet job completed successfully.");
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

    private void writeDataFrameToParquet(Dataset<Row> dataFrame) {
        log.info("Writing DataFrame to Parquet file...");
        dataFrame.write()
                .mode("overwrite")
                .parquet(PARQUET_FILE_PATH);
    }

}
