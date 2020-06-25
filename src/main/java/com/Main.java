package com;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // SparkConf conf = new SparkConf().setAppName("StaringSpark").setMaster("local[*]");
        // JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
        dataset.show();
        Long numberOfRows = dataset.count();
        System.out.println("There are " + numberOfRows + " records");
    }
}
