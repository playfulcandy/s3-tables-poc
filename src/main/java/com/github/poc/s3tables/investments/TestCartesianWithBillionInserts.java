package com.github.poc.s3tables.investments;

import com.github.poc.s3tables.investments.utils.DateUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.util.List;

import static com.github.poc.s3tables.investments.utils.SparkUtils.startSession;
import static org.apache.spark.sql.functions.lit;

@Slf4j
public class TestCartesianWithBillionInserts {

    public static void main(String[] args) throws NoSuchTableException {
        SparkSession spark = startSession("s3Test");

        log.info("S3 Table POC - Starting cartesian of 1 million × 1000");

        String sql = """
                SELECT username as username, name as stock_name
                FROM s3tablesbucket.investments.accounts
                LEFT JOIN s3tablesbucket.investments.stocks
                ON 1=1
        """;
        
        log.info("S3 Table POC - Running the following sql: {}", sql);

        Dataset<Row> cartesian = spark.sql(sql);

        log.info("S3 Table POC - Cartesian Join and read finished");

        cartesian = cartesian.withColumn("create_time", lit(DateUtils.getTime()));

        log.info(
                "S3 Table POC - Schema: {}",
                List.of(cartesian.schema().fieldNames())
        );

        log.info("S3 Table POC - Writing 1 billion records");

        cartesian.writeTo("s3tablesbucket.investments.cartesian")
                .option("batchsize", "1000000")
                .append();

        log.info("S3 Table POC - Writing 1 billion records finished");
    }
}
