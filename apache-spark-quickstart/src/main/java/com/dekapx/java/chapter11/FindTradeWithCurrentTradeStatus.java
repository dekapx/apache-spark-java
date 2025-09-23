package com.dekapx.java.chapter11;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.expressions.Window.partitionBy;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;

public class FindTradeWithCurrentTradeStatus {
    private static final String TRADE_ID = "tradeId";
    private static final String TRADE_DATE = "tradeDate";
    private static final String ROW_NUMBER = "row_number";

    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        Dataset<Row> tradesDF = readTradesCsv(spark);
        tradesDF.show();

        // filter trades with their current trade status
        Dataset<Row> currentTradesDF = tradesDF
                .withColumn(ROW_NUMBER, row_number()
                        .over(partitionBy(TRADE_ID).orderBy(col(TRADE_DATE).desc())))
                .filter(col(ROW_NUMBER).equalTo(1))
                .drop(ROW_NUMBER);
        currentTradesDF.show();
    }

    private static Dataset<Row> readTradesCsv(SparkSession spark) {
        String filePath = "src/main/resources/chapter11/sample-trades.csv";
        return spark.read()
                .option("header", "true")
                .csv(filePath);
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("ApacheSparkQuickStart")
                .master("local[*]")
                .getOrCreate();
    }
}
