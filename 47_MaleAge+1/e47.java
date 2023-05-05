package it.polito.bigdata.spark.exercise47;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDriver{
    public static void main(String[] args){
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        String inputpath;
        String outputpath;

        inputpath args[0];
        outputpath args[1];

        SparkSession ss = SparkSession.builder().master("local").appName("e47").getOrCreate();

        Dataset<Row> profiles = ss.read().format("csv").option("header",true).option("inferSchema", true).load(inputpath);

        Dataset<Profile> datasetprofiles = profiles.as(Encoders.bean(Profile.class));

        datasetprofiles.printSchema();
        datasetprofiles.show();

        datasetprofiles.createOrReplaceTempView("PRO")

        Dataset<NameAge> selectedANDchanged = ss.sql("SELECT name, age+1 as age FROM PRO WHERE gender='male ORDER BY age desc,name asc").as(Encoders.bean(NameAge.class));

        selectedANDchanged.printSchema();
        selectedANDchanged.show();

        selectedANDchanged.write().format("csv").option("header",false).save(outputpath);

        ss.stop();
    }
}
