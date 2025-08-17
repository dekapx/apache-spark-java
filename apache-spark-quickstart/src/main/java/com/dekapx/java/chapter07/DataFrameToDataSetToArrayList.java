package com.dekapx.java.chapter07;

import com.dekapx.java.chapter07.model.Trade;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class DataFrameToDataSetToArrayList {

    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        // Read the CSV file into a DataFrame
        Dataset<Row> allTradesDF = readCsvFile(spark);
        // Convert the DataFrame to a Dataset<Row>
        List<Row> rows = allTradesDF.toJavaRDD().collect();
        // Convert the Dataset<Row> to a List of Trade objects
        List<Trade> trades = new ArrayList<>();
        rows.forEach(row -> {
            Long tradeId = Long.valueOf(row.getAs("trade_id"));
            String type = row.getAs("type");
            Double price = Double.valueOf(row.getAs("price"));
            Integer quantity = Integer.valueOf(row.getAs("quantity"));
            String location = row.getAs("location");
            Date tradeDate = parseDate(row.getAs("trade_date"));
            trades.add(new Trade(tradeId, type, price, quantity, location, tradeDate));
        });
        // Print the trades
        trades.forEach(trade -> System.out.println(trade));
    }

    private static Date parseDate(String dateStr) {
        try {
            return new java.text.SimpleDateFormat("yyyy-MM-dd").parse(dateStr);
        } catch (java.text.ParseException e) {
            return null;
        }
    }

    private static Dataset<Row> readCsvFile(SparkSession spark) {
        String filePath = "src/main/resources/chapter07/allTrades.csv";
        return spark.read()
                .option("header", "true")
                .csv(filePath)
                .filter(col("trade_id").isNotNull());
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("ApacheSparkQuickStart")
                .master("local[*]")
                .getOrCreate();
    }
}
