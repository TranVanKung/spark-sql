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
        spark.udf().register("hasPassed",
                (String grade, String subject) -> {
                    if (subject.equals("Biology")) {
                        if (grade.startsWith("A")) {
                            return true;
                        }

                        return false;
                    }
                    
                    return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
                },
                DataTypes.BooleanType
        );

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

        dataset = dataset.withColumn("pass",
                functions.callUDF("hasPassed", functions.col("grade"), functions.col("subject"))
        );

        dataset.show();
    }
}
