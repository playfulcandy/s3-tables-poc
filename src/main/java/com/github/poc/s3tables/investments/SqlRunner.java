package com.github.poc.s3tables.investments;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.github.poc.s3tables.investments.utils.SparkUtils.startSession;

@Slf4j
public class SqlRunner {
    public static void main(String[] args) {
        if (args.length == 0) {
            log.info("No Args");
            return;
        }

        SparkSession spark = startSession("s3Test");

        for (String sql : args) {
            sql = sql.replaceAll("SPACE_TEXT", " ");
            log.info("Running SQL: {}", sql);
            Dataset<Row> result = spark.sql(sql);
            result.show();
        }
    }
}
