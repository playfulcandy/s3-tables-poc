package com.github.poc.s3tables.investments.utils;

import org.apache.spark.sql.SparkSession;

public class SparkUtils {
    public static SparkSession startSession(String sessionName) {
        return SparkSession.builder()
                .appName(sessionName)
                .config(
                        "spark.sql.catalog.s3tablesbucket",
                        "org.apache.iceberg.spark.SparkCatalog"
                ).config(
                        "spark.sql.catalog.s3tablesbucket.catalog-impl",
                        "software.amazon.s3tables.iceberg.S3TablesCatalog"
                ).config(
                        "spark.sql.catalog.s3tablesbucket.warehouse",
                        "arn:aws:s3tables:us-east-1:640104139613:bucket/poc"
                ).config(
                        "spark.sql.extensions",
                        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
                ).config(
                        "spark.hadoop.hive.metastore.client.factory.class",
                        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                ).getOrCreate();
    }
}
