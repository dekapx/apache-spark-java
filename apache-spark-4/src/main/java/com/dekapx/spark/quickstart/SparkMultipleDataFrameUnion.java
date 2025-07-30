package com.dekapx.spark.quickstart;

import com.dekapx.spark.model.Person;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

/**
 * vm-args: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 */
public class SparkMultipleDataFrameUnion {
    public static void main(String[] args) {
        SparkSession spark = createSparkSession();

        Dataset<Row> df1 = createFirstDataFrame(spark);
        df1.show();

        Dataset<Row> df2 = createSecondDataFrame(spark);
        df2.show();

        Dataset<Row> df3 = createThirdDataFrame(spark);
        df3.show();

        Dataset<Row> unionDF = df1.union(df2).union(df3);
        unionDF.show();
        spark.stop();
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("Spark DataFrame Union Example")
                .master("local[*]")
                .getOrCreate();
    }

    private static Dataset<Row> createFirstDataFrame(SparkSession spark) {
        Dataset<Row> df1 = spark.createDataFrame(Arrays.asList(
                new Person("Alice", 30),
                new Person("Bob", 25)
        ), Person.class);
        return df1;
    }

    private static Dataset<Row> createSecondDataFrame(SparkSession spark) {
        Dataset<Row> df2 = spark.createDataFrame(Arrays.asList(
                new Person("Charlie", 35),
                new Person("David", 28)
        ), Person.class);
        return df2;
    }

    private static Dataset<Row> createThirdDataFrame(SparkSession spark) {
        Dataset<Row> df3 = spark.createDataFrame(Arrays.asList(
                new Person("Eve", 22),
                new Person("Frank", 40)
        ), Person.class);
        return df3;
    }
}
