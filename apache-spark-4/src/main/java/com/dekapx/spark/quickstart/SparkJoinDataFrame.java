package com.dekapx.spark.quickstart;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

/**
 * vm-args: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 */
public class SparkJoinDataFrame {
    private static final String SPARK_APP_NAME = "SparkDataFrameJoin";
    private static final String SPARK_MASTER = "local[*]";
    private static final String HEADER = "header";
    private static final String INFER_SCHEMA = "inferSchema";
    private static final String PERSON_CSV = "src/main/resources/data/person.csv";
    private static final String STATES_CSV = "src/main/resources/data/states.csv";

    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        Dataset<Row> personDataframe = createDataFrame(spark, PERSON_CSV);
        Dataset<Row> stateDataframe = createDataFrame(spark, STATES_CSV);
        Dataset<Row> joinedDF = personDataframe.join(stateDataframe, "state_code")
                .select(personDataframe.col("first_name"),
                        personDataframe.col("last_name"),
                        personDataframe.col("age"),
                        personDataframe.col("city"),
                        stateDataframe.col("state_name"))
                .withColumn("Full Name",
                        concat(col("first_name"),
                                lit(", "),
                                col("last_name")))
                .withColumn("Age", personDataframe.col("age"))
                .withColumn("City", personDataframe.col("city"))
                .withColumn("State", stateDataframe.col("state_name"))
                .filter(col("age") // filter age between 22 and 38
                        .gt(22)
                        .and(col("age")
                                .lt(38)))
                .drop(personDataframe.col("first_name"))
                .drop(personDataframe.col("last_name"))
                .drop(personDataframe.col("age"))
                .drop(stateDataframe.col("state_name"))
                .orderBy(col("age").asc());
        joinedDF.printSchema();
        joinedDF.show();
        spark.stop();
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName(SPARK_APP_NAME)
                .master(SPARK_MASTER)
                .getOrCreate();
    }

    private static Dataset<Row> createDataFrame(SparkSession spark, String path) {
        return spark
                .read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(path);
    }
}
