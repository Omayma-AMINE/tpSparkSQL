package org.example;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;


public class Application {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("tpSparkSQL").master("local[*]").getOrCreate();

       Dataset<Row> incidents = sparkSession.read().option("header","true").csv("incidents.csv");
        //la liste des incidents
        incidents.show();
        //le nombre des incidents par service
        incidents.groupBy("Service").count().show();
        //les deux annee ou il y a plus d'incident
        incidents = incidents.withColumn("Year", year(to_date(col("Date"), "yyyy-MM-dd")));
        incidents.createOrReplaceTempView("incidentsYear");
        Dataset<Row> yearsInci = sparkSession.sql("SELECT Year, count(*) AS TotalIncident from incidentsYear group by Year order by TotalIncident desc limit 2");
        yearsInci.show();







    }
}
