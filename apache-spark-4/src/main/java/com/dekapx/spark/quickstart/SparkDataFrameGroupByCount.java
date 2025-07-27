package com.dekapx.spark.quickstart;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

public class SparkDataFrameGroupByCount {
    private static final String FILE_FORMAT = "csv";
    private static final String HEADER = "header";
    private static final String ALL_TRADES_FILE_PATH = "src/main/resources/data/allTrades.csv";

    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        Dataset<Row> allTradesDF = createAllTradesDataFrame(spark);
        allTradesDF.show();
        Dataset<Row> groupedDF = allTradesDF.groupBy("type").count();
        groupedDF.show();
    }

    private static Dataset<Row> createAllTradesDataFrame(SparkSession spark) {
        return spark
                .read()
                .format(FILE_FORMAT)
                .option(HEADER, true)
                .load(ALL_TRADES_FILE_PATH)
                .withColumn("valid_trade", lit(false))
                .filter(col("trade_id").isNotNull());
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("SparkDataFrameGroupByCount")
                .master("local[*]")
                .getOrCreate();
    }
}
