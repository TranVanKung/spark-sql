package com;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();

        spark.conf().set("spark.sql.shuffle.partitions", "10");

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

        dataset.createOrReplaceTempView("logging_table");
        Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total " +
                "from logging_table group by level, date_format(datetime, 'MMMM') order by cast(first(date_format(datetime, 'M')) as int), level");

//        dataset = dataset.select(functions.col("level"),
//                functions.date_format(functions.col("datetime"), "MMMM").alias("month"),
//                functions.date_format(functions.col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType)
//        );
//        dataset = dataset.groupBy("level", "month", "monthnum").count().as("total").orderBy("monthnum");
//        dataset.drop("monthnum");

        results.show();
//        dataset.show();

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

    }
}
