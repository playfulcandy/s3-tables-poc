package com.github.poc.s3tables.investments.randomdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.DataTypes;

import static com.github.poc.s3tables.investments.utils.DatasetCreator.createDataset;
import static com.github.poc.s3tables.investments.utils.RandomUtils.createRandomPrice;
import static com.github.poc.s3tables.investments.utils.SparkUtils.startSession;
import static org.apache.spark.sql.functions.callUDF;

public class BulkCreateRandomStocks {

    public static void main(String[] args) throws NoSuchTableException {
        SparkSession sparkSession = startSession("s3Test");
        Dataset<Row> stocks = createDataset(
                sparkSession,
                "stock",
                1000
        );
        stocks = stocks.withColumnRenamed("stock", "name");
        sparkSession.udf().register(
                "price",
                (UDF0<?>) () -> createRandomPrice(100.0, 1000.0),
                DataTypes.DoubleType
        );
        stocks = stocks.withColumn("price", callUDF("price"));

        sparkSession.sql("DELETE FROM s3tablesbucket.investments.`stocks`");
        stocks.writeTo("s3tablesbucket.investments.`stocks`").append();
    }
}
