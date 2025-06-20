package com.dekapx.spark.quickstart;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * vm-args: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 */
public class SparkJsonRead {
    public static final String HEADER = "header";
    public static final String MULTILINE = "multiline";
    public static final String JSON_FILE_PATH = "src/main/resources/data/car-inventory.json";

    public static void main(String[] args) {
        SparkSession sparkSession = createSparkSession();
        Dataset<Row> dataFrame = buildDataFrame(sparkSession);
        dataFrame.printSchema();
        dataFrame.show();
    }

    private static Dataset<Row> buildDataFrame(SparkSession sparkSession) {
        Dataset<Row> dataFrame = sparkSession
                .read()
                .option(HEADER, true)
                .option(MULTILINE, true)
                .json(JSON_FILE_PATH);
        return dataFrame;
    }

    private static SparkSession createSparkSession() {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("SparkJsonRead")
                .master("local[*]")
                .getOrCreate();
        return sparkSession;
    }
}
