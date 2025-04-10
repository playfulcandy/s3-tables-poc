package com.github.poc.s3tables.investments;

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

public class BulkCreateRandomHoldings {
    public static void main(String[] args) throws NoSuchTableException {
        SparkSession sparkSession = startSession("s3Test");
        Dataset<Row> stocks = createDataset(
                sparkSession,
                "stock",
                1000
        );
        Dataset<Row> usernames = createDataset(
                sparkSession,
                "username",
                100000
        );
        List<String> usernameList = usernames.collectAsList()
                .stream()
                .map(row -> row.getString(row.fieldIndex("username")))
                .toList();
        List<String> stockList = stocks.collectAsList()
                .stream()
                .map(row -> row.getString(row.fieldIndex("stock")))
                .toList();

        List<Row> holdingsList = new ArrayList<>(usernameList.size() * 15);

        for (String holder : usernameList) {
            Set<String> selected = getRandomSubset(15, stockList);
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
