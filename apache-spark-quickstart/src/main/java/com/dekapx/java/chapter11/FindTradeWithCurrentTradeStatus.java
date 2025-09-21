package com.dekapx.java.chapter11;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;

public class FindTradeWithCurrentTradeStatus {
    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        Dataset<Row> tradesDF = readTradesCsv(spark);
        tradesDF.show();

        // filter trades with their current trade status
        Dataset<Row> currentTradesDF = tradesDF
                .withColumn("row_number", row_number()
                        .over(Window.partitionBy("tradeId").orderBy(col("tradeDate").desc())))
                .filter(col("row_number").equalTo(1))
                .drop("row_number");
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
