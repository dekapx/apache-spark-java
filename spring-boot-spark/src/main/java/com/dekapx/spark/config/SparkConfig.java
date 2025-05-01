package com.dekapx.spark.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {
    @Value("${spark.app.name}")
    private String sparkAppName;

    @Value("${spark.app.master}")
    private String sparkAppMaster;

    @Bean
    public SparkSession sparkSession() {
        return SparkSession.builder()
                .appName(sparkAppName)
                .master(sparkAppMaster)
                .getOrCreate();
    }
}
