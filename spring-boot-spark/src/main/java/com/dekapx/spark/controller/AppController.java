package com.dekapx.spark.controller;

import com.dekapx.spark.service.CsvReaderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static com.dekapx.spark.common.ResourceUrls.BASE_URL;
import static com.dekapx.spark.common.ResourceUrls.INFO_URL;

@Slf4j
@RestController
@RequestMapping(BASE_URL)
@RequiredArgsConstructor
public class AppController {
    private final CsvReaderService csvReaderService;

    @GetMapping(INFO_URL)
    public String getInfo() {
        log.info("Apache Spark API v1.0");
        this.csvReaderService.readCsvFile();
        return "Apache Spark API v1.0";
    }
}
