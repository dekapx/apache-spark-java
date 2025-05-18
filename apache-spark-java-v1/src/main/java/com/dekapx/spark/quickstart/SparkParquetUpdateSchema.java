package com.dekapx.spark.quickstart;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * vm-args: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 */
public class SparkParquetUpdateSchema {
    public static final String FILE_FORMAT = "csv";
    public static final String FILE_HEADER = "header";
    private static final String CSV_FILE_PATH = "src/main/resources/data/employee.csv";
    private static final String PARQUET_FILE_PATH = "src/main/resources/data/employee.parquet";

    public static void main(String[] args) {
        SparkSession spark = createSparkSession();

        Dataset<Row> employeeDF = buildDataFrame(spark);
        employeeDF.write()
                .mode(SaveMode.Overwrite)
                .parquet(PARQUET_FILE_PATH);

        employeeDF = spark.read()
                .parquet(PARQUET_FILE_PATH);
        employeeDF.printSchema();
        employeeDF.show();

        // update parquet schema with new column
        employeeDF = employeeDF
                .withColumn("employment_type", functions.lit("full-time"))
                .select(employeeDF.col("*"), functions.col("employment_type").cast(DataTypes.StringType));
        employeeDF.printSchema();
        employeeDF.show();
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("SparkJoinFilter")
                .master("local[*]")
                .getOrCreate();
    }

    private static Dataset<Row> buildDataFrame(SparkSession sparkSession) {
        return sparkSession
                .read()
                .format(FILE_FORMAT)
                .option(FILE_HEADER, true)
                .load(CSV_FILE_PATH);
    }
}
