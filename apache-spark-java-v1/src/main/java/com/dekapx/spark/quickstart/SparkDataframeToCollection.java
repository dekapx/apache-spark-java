package com.dekapx.spark.quickstart;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

/**
 * vm-args: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 */
public class SparkDataframeToCollection {
    private static final String FILE_FORMAT = "csv";
    private static final String HEADER = "header";
    private static final String ALL_TRADES_FILE_PATH = "src/main/resources/data/allTrades.csv";

    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        Dataset<Row> allTradesDF = createAllTradesDataFrame(spark);
        List<Row> rows = allTradesDF.toJavaRDD().collect();
        rows.forEach(row -> {
            String tradeId = row.getAs("trade_id");
            boolean isValidTrade = row.getAs("valid_trade");
            System.out.println("Trade ID: " + tradeId + ", Valid Trade: " + isValidTrade);
        });
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
                .appName("SparkDataframeToCollection")
                .master("local[*]")
                .getOrCreate();
    }
}
