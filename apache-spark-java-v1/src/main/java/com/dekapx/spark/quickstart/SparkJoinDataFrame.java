package com.dekapx.spark.quickstart;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/**
 * vm-args: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 */
public class SparkJoinDataFrame {
    private static final String SPARK_APP_NAME = "SparkDataFrameJoin";
    private static final String SPARK_MASTER = "local[*]";
    private static final String PERSON_CSV = "src/main/resources/data/person.csv";
    private static final String STATES_CSV = "src/main/resources/data/states.csv";
    private static final String HEADER = "header";
    private static final String INFER_SCHEMA = "inferSchema";

    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        Dataset<Row> personDataframe = createDataFrame(spark, PERSON_CSV);
        Dataset<Row> stateDataframe = createDataFrame(spark, STATES_CSV);
        Dataset<Row> joinedDF = personDataframe.join(stateDataframe, "state_code")
                .select(personDataframe.col("first_name"),
                        personDataframe.col("last_name"),
                        personDataframe.col("age"),
                        stateDataframe.col("state_name"))
                .withColumn("state", col("state_name"))
                .drop("state_name")
                .filter(col("age")
                        .gt(22)
                        .and(col("age")
                                .lt(38)))
                .orderBy(col("age").desc());
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
