package com.dekapx.spark.quickstart;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkCsvToParquetJob {
    private static final String CSV_FILE_FORMAT = "csv";
    private static final String CSV_FILE_HEADER = "header";
    private static final String CSV_FILE_PATH = "src/main/resources/data/sample-trades.csv";
    private static final String PARQUET_FILE_PATH = "src/main/resources/output/sample.parquet";

    /**
     * vm-args: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
     */
    public static void main(String[] args) {
        SparkCsvToParquetJob job = new SparkCsvToParquetJob();
        job.trigger();
    }

    public void trigger() {
        SparkSession sparkSession = createSparkSession();
        Dataset<Row> dataFrame = csvToDataFrame(sparkSession);
        dataFrame.show();
        writeDataFrameToParquet(dataFrame);
        Dataset<Row> parquetDataFrame = sparkSession
                .read()
                .parquet(PARQUET_FILE_PATH);
        parquetDataFrame.show();
    }

    private SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("SparkCsvToParquetJob")
                .master("local[*]")
                .getOrCreate();
    }

    private Dataset<Row> csvToDataFrame(SparkSession sparkSession) {
        return sparkSession
                .read()
                .format(CSV_FILE_FORMAT)
                .option(CSV_FILE_HEADER, true)
                .load(CSV_FILE_PATH);
    }

    private void writeDataFrameToParquet(Dataset<Row> dataFrame) {
        dataFrame.write()
                .mode("overwrite")
                .parquet(PARQUET_FILE_PATH);
    }
}
