package com;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();
//        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
        Dataset<Row> dataset = spark.read().option("header", true).option("inferSchema", true).csv("src/main/resources/exams/students.csv");

        dataset = dataset.groupBy("subject").agg(
                functions.max(functions.col("score")).alias("maxScore"),
                functions.min(functions.col("score")).alias("minScore")
        );
        dataset.show();
    }
}
