package it.polito.bigdata.spark.exercise48;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDriver{
    public class void main(String[] args){

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        
        String inputpath;
        String outputpath;

        inputpath = args[0];
        outputpath = args[1];

        SparkSession ss = SparkSession.builder().master("local").appName("e48").getOrCreate();

        Dataset<Row> infile = ss.read().format("csv").option("header",true).option("inferSchema", true).load(inputpath);

        Dataset<Profile> datasetprofiles = infile.as(Encoders.bean(Profile.class));

        datasetprofiles.inferSchema();
        datasetprofiles.show();

        datasetprofiles.createOrReplaceTempView("profiles");
        
        Dataset<NameAgeAverage> average = ss.sql( "SELECT name,avg(age) as average"
                 +"FROM profiles"
                 +"GROUP BY name"
                 +"HAVING count(*)>= 2").as(Encoders.bean(NameAgeAverage.class));
       
        average.inferSchema();
        average.show();

        average.write().format("csv").option("header", false).save(outputpath);
        ss.stop();

    }
}