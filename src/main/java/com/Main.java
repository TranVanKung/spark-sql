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

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
        SimpleDateFormat input = new SimpleDateFormat("MMMM");
        SimpleDateFormat output = new SimpleDateFormat("M");

        spark.udf().register("monthNum",
                (String month) -> {
                    Date inputDate = input.parse(month);
                    return Integer.parseInt(output.format(inputDate));
                },
                DataTypes.IntegerType);

        dataset.createOrReplaceTempView("logging_table");

        Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total " +
                "from logging_table group by level, date_format(datetime, 'MMMM') order by monthNum(month), level");

        results.show();
    }
}
