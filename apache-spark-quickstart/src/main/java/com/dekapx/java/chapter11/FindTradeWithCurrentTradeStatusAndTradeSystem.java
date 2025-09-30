package com.dekapx.java.chapter11;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.expressions.Window.partitionBy;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;

public class FindTradeWithCurrentTradeStatusAndTradeSystem {
    private static final String TRADE_ID = "tradeId";
    private static final String TRADE_DATE = "tradeDate";
    private static final String ROW_NUMBER = "row_number";

    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        Dataset<Row> tradesDF = readTradesCsv(spark);
        tradesDF.show();

        Dataset<Row> currentTradesDF = filterByTradeSystem(tradesDF, "Broadridge");
        currentTradesDF.show();
    }

    private static Dataset<Row> filterByTradeSystem(Dataset<Row> tradesDF, String tradeSystem) {
        Dataset<Row> currentTradesDF = tradesDF
                .withColumn(ROW_NUMBER, row_number()
                        .over(partitionBy(TRADE_ID).orderBy(col(TRADE_DATE).desc())))
                .filter((col(ROW_NUMBER).equalTo(1).and(col("tradeSystem").equalTo(tradeSystem)))
                        .or(col("tradeSystem").notEqual(tradeSystem)))
                .drop(ROW_NUMBER);
        return currentTradesDF;
    }

    private static Dataset<Row> readTradesCsv(SparkSession spark) {
        String filePath = "src/main/resources/chapter11/sample-trades-02.csv";
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
