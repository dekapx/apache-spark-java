package com.dekapx.spark.quickstart;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkReadWriteParquet {
    public static final String FILE_FORMAT = "csv";
    public static final String FILE_PATH = "src/main/resources/data/movies-data.txt";
    public static final String FILE_HEADER = "header";
    public static final String PARQUET_FILE_PATH = "src/main/resources/output/sample.parquet";

    public static void main(String[] args) {
        SparkSession sparkSession = createSparkSession();
        Dataset<Row> df = buildDataFrame(sparkSession);
        df.write()
                .mode(SaveMode.Overwrite)
                .parquet(PARQUET_FILE_PATH);

        Dataset<Row> df2 = sparkSession
                .read()
                .parquet(PARQUET_FILE_PATH);
        df2.show();
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("SparkReadWriteParquet")
                .master("local[*]")
                .getOrCreate();
    }

    private static Dataset<Row> buildDataFrame(SparkSession sparkSession) {
        return sparkSession
                .read()
                .format(FILE_FORMAT)
                .option(FILE_HEADER, true)
                .load(FILE_PATH);
    }
}
