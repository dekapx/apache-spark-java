package com.dekapx.spark.quickstart;

import com.dekapx.spark.model.Language;
import com.dekapx.spark.model.LanguageType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.MapFunction;
import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static com.dekapx.spark.model.LanguageTypeMapper.getLanguageType;

/**
 * vm-args: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 */
public class StringDataSetMapper {
    public static void main(String[] args) {
        String[] data = {"Java", "Python", "Scala", "C++", "JavaScript"};
        SparkSession sparkSession = createSparkSession();
        Dataset<String> dataset = sparkSession.createDataset(Arrays.asList(data), Encoders.STRING());
        dataset.printSchema();
        dataset.show();
        dataset.orderBy(col("value")).show();

        Dataset<Language> languageDataset = dataset.map(mapper, Encoders.bean(Language.class));
        languageDataset.printSchema();
        languageDataset.show();
    }

    private static MapFunction<String, Language> mapper = (value) -> {
        LanguageType type = getLanguageType(value);
        return new Language(value, type);
    };

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName("StringDataSetMapper")
                .master("local[*]")
                .getOrCreate();
    }
}
