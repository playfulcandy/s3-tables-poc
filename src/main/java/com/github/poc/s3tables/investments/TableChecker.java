package com.github.poc.s3tables.investments;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.github.poc.s3tables.investments.utils.SparkUtils.startSession;

@Slf4j
public class TableChecker {
    public static void main(String[] args) {
        if (args.length == 0) {
            log.info("No Args");
            return;
        }

        SparkSession spark = startSession("s3Test");

        for (String table : args) {
            table = table.replaceAll("SPACE_TEXT", " ");
            log.info("Checking: {}", table);
            Dataset<Row> result = spark.table(table);
            result.show();
        }

        spark.close();
    }
}
