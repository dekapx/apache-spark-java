package com.dekapx.java.chapter10;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

public class JoinCompareFilterDataFrames {
    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        Dataset<Row> allTradesDF = readAllTradesCsv(spark);
        Dataset<Row> validTradesDF = readValidTradesCsv(spark);

       filterValidTrades(allTradesDF, validTradesDF);

        spark.stop();
    }

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
    }

    private static Dataset<Row> readAllTradesCsv(SparkSession spark) {
        String filePath = "src/main/resources/chapter10/allTrades.csv";
        return spark.read()
                .option("header", "true")
                .csv(filePath)
                .withColumn("valid_trade", lit(false))
                .filter(col("trade_id").isNotNull());
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
