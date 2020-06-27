package com;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

        dataset = dataset.select(
                functions.col("level"),
                functions.date_format(functions.col("datetime"), "MMMM").alias("month"),
                functions.date_format(functions.col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType)
        );

        Object[] months = new Object[]{"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December", "Test"};
        List<Object> columns = Arrays.asList(months);

        dataset = dataset.groupBy("level").pivot("month", columns).count().na().fill(0);

        dataset.show(100);
    }
}
