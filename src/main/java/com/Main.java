package com;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

        dataset.createOrReplaceTempView("logging_table");
        Dataset<Row> results = spark.sql
                ("select level, date_format(datetime, 'MMMM') as month, cast(first(date_format(datetime, 'M')) as int) as monthnum, count(1) as total " +
                        "from logging_table group by level, date_format(datetime,'MMMM') order by monthnum");

        results.createOrReplaceTempView("results_table");
        results = results.drop("monthnum");

        results.show(100);
    }
}
