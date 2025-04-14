package com.github.poc.s3tables.investments;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.github.poc.s3tables.investments.utils.DatasetCreator.createDataset;
import static com.github.poc.s3tables.investments.utils.RandomUtils.getRandomSubset;
import static com.github.poc.s3tables.investments.utils.SparkUtils.startSession;

@Slf4j
public class BulkCreateRandomHoldings {
    public static void main(String[] args) throws NoSuchTableException {
        SparkSession sparkSession = startSession("s3Test");
        Dataset<Row> stocks = sparkSession.sql("""
                SELECT
                    name AS stock
                FROM s3tablesbucket.investments.`stocks`
        """);
        Dataset<Row> usernames = sparkSession.sql("""
                SELECT
                    username
                FROM s3tablesbucket.investments.`accounts`
        """);

        log.info("S3 Table POC - Reading the tables finished");

        List<String> usernameList = usernames.collectAsList()
                .stream()
                .map(row -> row.getString(row.fieldIndex("username")))
                .toList();
        List<String> stockList = stocks.collectAsList()
                .stream()
                .map(row -> row.getString(row.fieldIndex("stock")))
                .toList();

        log.info("S3 Table POC - Transformation of datasets finished");

        List<Row> holdingsList = new ArrayList<>(usernameList.size() * 25);

        log.info("S3 Table POC - Allocating array of 25 million finished");

        for (String holder : usernameList) {
            Set<String> selected = getRandomSubset(25, stockList);
            for (String stockName : selected) {
                Integer amountHeld = 20 + ((int) (Math.random() * 150));

                holdingsList.add(
                        RowFactory.create(holder, stockName, amountHeld.doubleValue())
                );
            }
        }

        StructType holdingSchema = new StructType()
                .add("holder", DataTypes.StringType)
                .add("stockname", DataTypes.StringType)
                .add("amount", DataTypes.DoubleType);

        Dataset<Row> holdings = sparkSession.createDataFrame(
                holdingsList, holdingSchema
        );

        sparkSession.sql("DELETE FROM s3tablesbucket.investments.`holdings`");
        holdings.writeTo("s3tablesbucket.investments.`holdings`").append();
    }
}
