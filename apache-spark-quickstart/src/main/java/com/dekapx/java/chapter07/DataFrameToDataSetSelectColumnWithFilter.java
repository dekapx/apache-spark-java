package com.dekapx.java.chapter07;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

public class DataFrameToDataSetSelectColumnWithFilter {
    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        Dataset<Row> allTradesDF = readCsvFile(spark);
        List<Row> rows = allTradesDF.toJavaRDD().collect();
        rows.forEach(row -> {
            String tradeId = row.getAs("trade_id");
            String type = row.getAs("type");
            String price = row.getAs("price");
            boolean isValidTrade = row.getAs("valid_trade");
            System.out.println("Trade ID: " + tradeId + ", Type: " + type + ", Price: " + price + ", Valid Trade: " + isValidTrade);
        });
    }

    private static Dataset<Row> readCsvFile(SparkSession spark) {
        String filePath = "src/main/resources/chapter07/allTrades.csv";
        return spark.read()
                .option("header", "true")
                .csv(filePath)
                .filter(col("trade_id").isNotNull())
                .select("trade_id", "type", "price")
                .withColumn("valid_trade", lit(false));
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("ApacheSparkQuickStart")
                .master("local[*]")
                .getOrCreate();
    }
}
