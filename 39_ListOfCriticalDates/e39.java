package it.polito.bigdata.spark.exercise39;

import org.apache.spark.api.java.*;
import scala.Tuple2;
import org.apache.spark.SparkConf;

public class SparkDriver{
    public static void main(String[] args){

        String inputpath;
        String outputpath;

        inputpath = args[0];
        outputpath = args[1];

        SparkConf conf = new SparkConf().setAppName("e39");

        JavaSparkContext obj = new JavaSparkContext(conf);

        JavaRDD <String> infile = obj.textFile(inputpath);

        JavaRDD <String> morethan50 = infile.filter(line ->{
            Double sValue;
            String[] parts= line.split(",");
            sValue = new Double(parts[2]);

            if(sValue > 50)
                return true;
            else
                return false;
        });

        JavaPairRDD<String, String> sensorpair = morethan50.mapToPair(line ->{
            String sDate;
            String sID;

            String[] parts= line.split(",");
            sDate = parts[1];
            sID = parts[0];

            Tuple2<String, String> pair = new Tuple2<String, String>(sID, sDate);

            return pair;
        });

        JavaPairRDD<String, Iterable<String>> listofDates = sensorpair. groupByKey();
        
        listofDates.saveAsTextFile(outputpath);

        obj.close();

    }
}