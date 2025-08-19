package com.dekapx.java.chapter10;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

public class JoinCompareDataFrames {
    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        Dataset<Row> allTradesDF = readAllTradesCsv(spark);
        Dataset<Row> validTradesDF = readValidTradesCsv(spark);

        // filter all trades with valid trades and add a new column to indicate if the trade is valid
        Dataset<Row> filteredDF = allTradesDF
                .join((Dataset<?>) broadcast(validTradesDF), allTradesDF.col("trade_id").equalTo(validTradesDF.col("trade_Id")), "left_outer")
                .select(allTradesDF.col("*"), validTradesDF.col("trade_Id"))
                .withColumn("valid_trade",
                        when((validTradesDF.col("trade_Id").isNotNull()), lit(true))
                                .otherwise(lit(false)))
                .drop(validTradesDF.col("trade_Id"));
        filteredDF.show();

        spark.stop();
    }


    private static Dataset<Row> readAllTradesCsv(SparkSession spark) {
        String filePath = "src/main/resources/chapter10/allTrades.csv";
        return spark.read()
                .option("header", "true")
                .csv(filePath);
    }

    private static Dataset<Row> readValidTradesCsv(SparkSession spark) {
        String filePath = "src/main/resources/chapter10/validTrades.csv";
        return spark.read()
                .option("header", "true")
                .csv(filePath)
                .select("trade_Id");
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("ApacheSparkQuickStart")
                .master("local[*]")
                .getOrCreate();
    }
}
