package com.github.poc.s3tables.investments.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class DatasetCreator {

    public static Dataset<Row> createDataset(
            SparkSession session,
            String columnName,
            int numElements
    ) {
        StructType schema = new StructType()
                .add(columnName, DataTypes.StringType);
        List<Row> values = createList(columnName, numElements)
                .stream()
                .map(RowFactory::create)
                .toList();
        return session.createDataFrame(values, schema);
    }

    private static List<String> createList(String prefix, int elements) {
        List<String> list = new ArrayList<>(elements);
        for (int i = 1; i <= elements; i++) {
            list.add(prefix + i);
        }
        return list;
    }
}
