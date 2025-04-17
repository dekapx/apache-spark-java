package com.dekapx.spark.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class CsvReaderService {
    public static final String CSV_FILE_FORMAT = "csv";
    public static final String CSV_FILE_PATH = "src/main/resources/data/movies-data.txt";
    public static final String FILE_HEADER = "header";

    private SparkSession sparkSession;

    public void readCsvFile() {
        log.info("Reading CSV file...");
        createSparkSession();
        Dataset<Row> dataFrame = buildDataFrame(sparkSession);
        dataFrame.printSchema();
        dataFrame.show();
        log.info("CSV file read successfully.");
    }

    private void createSparkSession() {
        this.sparkSession = SparkSession.builder()
                .appName("Spring Boot Spark App")
                .master("local[*]")
                .getOrCreate();
    }

    private static Dataset<Row> buildDataFrame(SparkSession sparkSession) {
        return sparkSession
                .read()
                .format(CSV_FILE_FORMAT)
                .option(FILE_HEADER, true)
                .load(CSV_FILE_PATH);
    }

}
