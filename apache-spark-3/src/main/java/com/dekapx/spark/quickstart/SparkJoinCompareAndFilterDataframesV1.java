package com.dekapx.spark.quickstart;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

/**
 * vm-args: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 */
public class SparkJoinCompareAndFilterDataframesV1 {
    private static final String FILE_FORMAT = "csv";
    private static final String HEADER = "header";
    private static final String ALL_TRADES_FILE_PATH = "src/main/resources/data/allTrades.csv";
    private static final String VALID_TRADES_FILE_PATH = "src/main/resources/data/validTrades.csv";


    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        Dataset<Row> allTradesDF = createAllTradesDataFrame(spark);
        Dataset<Row> validTradesDF = createValidTradesDataFrame(spark);

        // filter all trades with valid trades and add a new column to indicate if the trade is valid
        Dataset<Row> filteredDF = allTradesDF
                .join(broadcast(validTradesDF), allTradesDF.col("trade_id").equalTo(validTradesDF.col("trade_Id")), "left_outer")
                .select(allTradesDF.col("*"), validTradesDF.col("trade_Id"))
                .withColumn("valid_trade",
                        when((validTradesDF.col("trade_Id").isNotNull()), lit(true))
                        .otherwise(lit(false)))
                .drop(validTradesDF.col("trade_Id"));
        filteredDF.show();

        spark.stop();
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("SparkJoinFilter")
                .master("local[*]")
                .getOrCreate();
    }

    private static Dataset<Row> createAllTradesDataFrame(SparkSession spark) {
        return spark
                .read()
                .format(FILE_FORMAT)
                .option(HEADER, true)
                .load(ALL_TRADES_FILE_PATH);
    }

    private static Dataset<Row> createValidTradesDataFrame(SparkSession spark) {
        return spark
                .read()
                .format(FILE_FORMAT)
                .option(HEADER, true)
                .load(VALID_TRADES_FILE_PATH)
                .select("trade_Id");
    }
}
