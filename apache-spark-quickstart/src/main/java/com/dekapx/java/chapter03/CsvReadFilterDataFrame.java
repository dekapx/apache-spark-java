package com.dekapx.java.chapter03;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

public class CsvReadFilterDataFrame {
    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        Dataset<Row> dataFrame = buildDataFrame(spark);

        Dataset<Row> dataFrameGreaterThan = filterForGreaterThan(dataFrame, 90);
        dataFrameGreaterThan.show();

        Dataset<Row> dataFrameLessThan = filterForLessThan(dataFrame, 50);
        dataFrameLessThan.show();

        Dataset<Row> dataFrameBetween = filterForBetween(dataFrame, 50, 90);
        dataFrameBetween.show();

        Dataset<Row> dataFrameHighAndLowMarks = filterForForHighAndLowMarks(dataFrame);
        dataFrameHighAndLowMarks.show();

        Dataset<Row> dataFrameWithGrades = applyGrades(dataFrame);
        dataFrameWithGrades.show();

        Dataset<Row> dataFrameWithGradesForMarks = applyGradesForMarks(dataFrame);
        dataFrameWithGradesForMarks.show();

        spark.stop();
    }

    private static Dataset<Row> filterForGreaterThan(Dataset<Row> dataFrame, int marks) {
        return dataFrame
                .filter(col("marks").gt(marks))
                .select("first_name", "last_name", "marks", "city")
                .limit(10)
                .sort("first_name")
                .orderBy("first_name");
    }

    private static Dataset<Row> filterForLessThan(Dataset<Row> dataFrame, int marks) {
        return dataFrame
                .filter(col("marks").lt(marks))
                .select("first_name", "last_name", "marks", "city")
                .limit(10)
                .sort("first_name")
                .orderBy("first_name");
    }

    private static Dataset<Row> filterForBetween(Dataset<Row> dataFrame, int from, int to) {
        return dataFrame
                .filter(col("marks").between(from, to))
                .select("first_name", "last_name", "marks", "city")
                .limit(10)
                .sort("first_name")
                .orderBy("first_name");
    }

    private static Dataset<Row> filterForForHighAndLowMarks(Dataset<Row> dataFrame) {
        return dataFrame
                .withColumn("high_marks", col("marks").gt(90))
                .withColumn("low_marks", col("marks").lt(50))
                .select("first_name", "last_name", "marks", "city", "high_marks", "low_marks")
                .limit(10)
                .sort("first_name")
                .orderBy("first_name");
    }

    private static Dataset<Row> applyGrades(Dataset<Row> dataFrame) {
        return dataFrame
                .withColumn("grade",when((col("marks")
                        .gt(80)), lit("Pass"))
                        .otherwise(lit("Fail")))
                .select("first_name", "last_name", "marks", "city", "grade")
                .sort("marks")
                .orderBy("marks");
    }

    private static Dataset<Row> applyGradesForMarks(Dataset<Row> dataFrame) {
        return dataFrame
                .withColumn("grade",
                        when(col("marks").between(90, 100), lit("A+"))
                                .when(col("marks").between(80, 89), lit("A"))
                                .when(col("marks").between(70, 79), lit("B"))
                                .when(col("marks").between(60, 69), lit("C"))
                                .when(col("marks").between(50, 59), lit("D"))
                                .otherwise(lit("F")))
                .select("first_name", "last_name", "marks", "city", "grade")
                .sort("marks")
                .orderBy("marks");
    }

    private static Dataset<Row> buildDataFrame(SparkSession sparkSession) {
        return sparkSession
                .read()
                .format("csv")
                .option("header", true)
                .load("src/main/resources/chapter03/data.csv");
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("ApacheSparkQuickStart")
                .master("local[*]")
                .getOrCreate();
    }
}
