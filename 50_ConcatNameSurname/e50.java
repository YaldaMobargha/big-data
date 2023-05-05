package it.polito.bigdata.spark.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class SparkDriver{
    public static void main(String[] args){
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        String inputpath;
        String outputpath;

        inputpath = args[0];
        outputpath = args[1];

        SparkSession ss = SparkSession.builder().master("local").appName("e49").getOrCreate();

        Dataset<Row> infile = ss.read().format("csv").option("header",true).option("inferSchema",true).load(inputpath);

        infile.createOrReplaceTempView("profiles");

        ss.udf().register("Concat", (String name, String surname) -> new String(name+ " "+ surname),DataTypes.StringType);

        Dataset<Row> fullname = ss.sql("SELECT Concat(name, surname)as name_surname"
                                        +"FROM profiles");

        fullname.write().format("csv").option("header", true).save(outputpath);

        ss.stop();
    }
}