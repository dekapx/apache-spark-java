package com.dekapx.spark.quickstart;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/**
 * vm-args: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 */
public class SparkJoinDataFrame {
    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        Dataset<Row> personDataframe = createPersonDataFrame(spark);
        Dataset<Row> stateDataframe = createStatesDataFrame(spark);
        Dataset<Row> joinedDF = personDataframe.join(stateDataframe, "state_code")
                .select(personDataframe.col("first_name"),
                        personDataframe.col("last_name"),
                        personDataframe.col("age"),
                        stateDataframe.col("state_name"))
                .withColumn("state", col("state_name"))
                .drop("state_name")
                .filter(col("age").gt(30));
        joinedDF.printSchema();
        joinedDF.show();
        spark.stop();
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("Spark DataFrame Union Example")
                .master("local[*]")
                .getOrCreate();
    }

    private static Dataset<Row> createPersonDataFrame(SparkSession spark) {
        return spark
                .read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/person.csv");
    }

    private static Dataset<Row> createStatesDataFrame(SparkSession spark) {
        return spark
                .read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/states.csv");
    }
}
