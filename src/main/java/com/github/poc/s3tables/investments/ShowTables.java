package com.github.poc.s3tables.investments;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static com.github.poc.s3tables.investments.utils.SparkUtils.startSession;

@Slf4j
public class ShowTables {
    public static void main(String[] args) {
        SparkSession spark = startSession("s3Test");

        Dataset<Row> dataset = spark.sql("SHOW TABLES IN s3tablesbucket.investments");

        List<Row> rows = dataset.collectAsList();
        log.info("S3 Table POC - Show Tables Size: {}", rows.size());
        for (Row row: rows) {
            log.info("S3 Table POC - Show Tables Value: {}", row.toString());
        }

        spark.close();
    }
}
