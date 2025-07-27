package com.dekapx.spark.quickstart;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

/**
 * vm-args: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 */
public class SparkJoinCompareAndFilterDataframesV3 {
    private static final String FILE_FORMAT = "csv";
    private static final String HEADER = "header";
    private static final String ALL_TRADES_FILE_PATH = "src/main/resources/data/allTrades.csv";
    private static final String VALID_TRADES_FILE_PATH = "src/main/resources/data/validTrades.csv";


    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        Dataset<Row> allTradesDF = createAllTradesDataFrame(spark);
        Dataset<Row> validTradesDF = createValidTradesDataFrame(spark);
        filterValidTrades(allTradesDF, validTradesDF);
        spark.stop();
    }

    /**
     * Join all trades with valid trades and filter the rows
     * Add a new column "valid_trade" with true or false
     */
    private static void filterValidTrades(Dataset<Row> allTradesDF, Dataset<Row> validTradesDF) {
        Dataset<Row> filteredDF = allTradesDF
                .join((Dataset<?>) broadcast(validTradesDF), allTradesDF.col("trade_id")
                        .equalTo(validTradesDF.col("trade_Id")), "left_outer")
                .select(allTradesDF.col("*"), validTradesDF.col("trade_Id"))
                .withColumn("valid_trade",
                        when((allTradesDF.col("trade_Id")
                                .equalTo(validTradesDF.col("trade_Id"))), lit(true))
                        .otherwise(lit(false)))
                .drop(validTradesDF.col("trade_Id"));
        filteredDF.show();

        // TODO: find out how many rows are valid and invalid trades
        long validTradesCount = filteredDF.filter(col("valid_trade").equalTo(true)).count();
        long invalidTradesCount = filteredDF.filter(col("valid_trade").equalTo(false)).count();
        System.out.println("Number of valid trades: " + validTradesCount);
        System.out.println("Number of invalid trades: " + invalidTradesCount);
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("SparkJoinFilter")
                .master("local[*]")
                .getOrCreate();
    }

    /**
     * Create a DataFrame with all trades, filter rows with null or empty trade_id
     * Add a new column "valid_trade" with default value false
     */
    private static Dataset<Row> createAllTradesDataFrame(SparkSession spark) {
        return spark
                .read()
                .format(FILE_FORMAT)
                .option(HEADER, true)
                .load(ALL_TRADES_FILE_PATH)
                .withColumn("valid_trade", lit(false))
                .filter(col("trade_id").isNotNull());
    }

    /**
     * Create a DataFrame with valid trades only with trade_Id column
     */
    private static Dataset<Row> createValidTradesDataFrame(SparkSession spark) {
        return spark
                .read()
                .format(FILE_FORMAT)
                .option(HEADER, true)
                .load(VALID_TRADES_FILE_PATH)
                .select("trade_Id");
    }
}
