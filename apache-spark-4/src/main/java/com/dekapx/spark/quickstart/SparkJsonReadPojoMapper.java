package com.dekapx.spark.quickstart;

import com.dekapx.spark.model.Car;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * vm-args: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 */
public class SparkJsonReadPojoMapper {
    public static final String HEADER = "header";
    public static final String MULTILINE = "multiline";
    public static final String JSON_FILE_PATH = "src/main/resources/data/car-inventory.json";

    public static void main(String[] args) {
        SparkSession sparkSession = createSparkSession();
        Dataset<Row> dataFrame = buildDataFrame(sparkSession);
        System.out.println("------------------- DataFrame schema:");
        dataFrame.printSchema();
        dataFrame.show();

        System.out.println("------------------- Transformed DataFrame schema:");
        Dataset<Row> transformedDataFrame = transformedDataFrame(dataFrame);
        transformedDataFrame.printSchema();
        transformedDataFrame.show();

        mapToPojo(transformedDataFrame);
    }

    private static void mapToPojo(Dataset<Row> dataFrame) {
        Dataset<Car> carDataset = dataFrame.as(Encoders.bean(Car.class));
        System.out.println("------------------- Car Dataset schema:");
        carDataset.printSchema();
        carDataset.show();
    }

    private static Dataset<Row> transformedDataFrame(Dataset<Row> dataFrame) {
        Dataset<Row> transformedDataFrame = dataFrame
                .withColumn("fuelType", dataFrame.col("fuel_type").cast("string"))
                .withColumn("price", dataFrame.col("price").cast("double"))
                .withColumn("mileage", dataFrame.col("mileage").cast("int"))
                .drop("fuel_type");
        return transformedDataFrame;
    }

    private static Dataset<Row> buildDataFrame(SparkSession sparkSession) {
        Dataset<Row> dataFrame = sparkSession
                .read()
                .option(HEADER, true)
                .option(MULTILINE, true)
                .json(JSON_FILE_PATH);
        return dataFrame;
    }

    private static SparkSession createSparkSession() {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("SparkJsonReadPojoMapper")
                .master("local[*]")
                .getOrCreate();
        return sparkSession;
    }
}
