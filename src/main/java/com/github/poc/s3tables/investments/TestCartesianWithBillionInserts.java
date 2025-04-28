package com.github.poc.s3tables.investments;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static com.github.poc.s3tables.investments.utils.SparkUtils.startSession;

@Slf4j
public class TestCartesianWithBillionInserts {

    public static void main(String[] args) {
        SparkSession spark = startSession("s3Test");

        log.info("Starting cartesian of 1 million × 1000");

        Dataset<Row> cartesian = spark.sql("""
                SELECT
                    username as username,
                    name as stock_name
                FROM s3tablesbucket.investments.accounts
                LEFT JOIN s3tablesbucket.investments.stocks
                ON 1=1
        """);

        log.info("Cartesian Join and read finished");

        cartesian = cartesian.withColumnRenamed("name", "stock_name");

        log.info(
                "S3 Table POC - Schema: {}",
                List.of(cartesian.schema().fieldNames())
        );

        log.info("Writing 1 billion records");

        cartesian.writeTo("s3tablesbucket.investments.cartesian_test")
                .option("batchsize", "1000000")
                .createOrReplace();

        log.info("Writing 1 billion records finished");
    }
}
