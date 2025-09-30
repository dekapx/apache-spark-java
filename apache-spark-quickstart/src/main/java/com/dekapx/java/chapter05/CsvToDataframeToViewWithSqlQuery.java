package com.dekapx.java.chapter05;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvToDataframeToViewWithSqlQuery {
    public static void main(String[] args) {
        SparkSession spark = createSparkSession();

        Dataset<Row> dataFrame = csvToDataFrame(spark);
        dataFrame.createOrReplaceTempView("GLOBAL_STOCK");
        dataFrame.show();

        Dataset<Row> buyStockSummary = spark.sql("SELECT * FROM GLOBAL_STOCK WHERE type = 'BUY'");
        buyStockSummary.show();

        Dataset<Row> sellStockSummary = spark.sql("SELECT * FROM GLOBAL_STOCK WHERE type = 'SELL'");
        sellStockSummary.show();
    }


    private static Dataset<Row> csvToDataFrame(SparkSession spark) {
        String filePath = "src/main/resources/chapter05/sample-trades-01.csv";
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
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
