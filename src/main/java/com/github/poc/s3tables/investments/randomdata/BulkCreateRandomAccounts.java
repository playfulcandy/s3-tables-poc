package com.github.poc.s3tables.investments.randomdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.DataTypes;

import static com.github.poc.s3tables.investments.utils.DatasetCreator.createDataset;
import static com.github.poc.s3tables.investments.utils.RandomUtils.createRandomAlphaNumeric;
import static com.github.poc.s3tables.investments.utils.SparkUtils.startSession;
import static org.apache.spark.sql.functions.callUDF;

public class BulkCreateRandomAccounts {

    public static void main(String[] args) throws NoSuchTableException {
        SparkSession sparkSession = startSession("s3Test");
        Dataset<Row> usernames = createDataset(
                sparkSession,
                "username",
                1000000
        );
        sparkSession.udf().register(
                "password",
                (UDF0<?>) () -> createRandomAlphaNumeric(15),
                DataTypes.StringType
        );
        Dataset<Row> accounts = usernames.withColumn("password", callUDF("password"));

        sparkSession.sql("DELETE FROM s3tablesbucket.investments.`accounts`");
        accounts.writeTo("s3tablesbucket.investments.`accounts`").append();
    }
}
