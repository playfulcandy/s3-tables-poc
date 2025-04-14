package com.github.poc.s3tables.investments;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import static com.github.poc.s3tables.investments.utils.SparkUtils.startSession;
import static org.apache.spark.sql.functions.current_timestamp;

@Slf4j
public class JoinAccountsWithHoldingsAndStocks {
    public static void main(String[] args) throws NoSuchTableException {
        SparkSession spark = startSession("s3Test");

        log.info("S3 Table - Reads with Left Join Started");

        Dataset<Row> results = spark.sql("""
               SELECT
                   username, stockname, price, amount
               FROM s3tablesbucket.investments.`accounts`

               LEFT JOIN s3tablesbucket.investments.`holdings`
               ON username=holder

               LEFT JOIN s3tablesbucket.investments.`stocks`
               ON stockname=name
        """);

        log.info("S3 Table - Reads with Left Join Complete");

        results = results.withColumn("report_time", current_timestamp());

        log.info("S3 Table - Appending a million rows started");

        results.writeTo("s3tablesbucket.investments.reports").append();

        log.info("S3 Table - Appending a million rows complete");
    }
}
