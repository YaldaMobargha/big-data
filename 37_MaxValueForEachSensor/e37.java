package it.polito.bigdata.spark.exercise37;

import org.apache.spark.api.java.*;
import scala.Tuple2;
import org.apache.spark.SparkConf;

public class SparkDriver{
    public static void main(String[] args) {

        String inputpath;
        String outputpath;

        inputpath = args[0]:
        outputpath = args[1];

        SparkConf conf = new SparkConf().setAppName("e37");

        JavaSparkContext obj = new JavaSparkContext(conf);

        JavaRDD<String> infile = obj.textFile(inputpath);

        JavaPairRDD<String,Double> SensorANDValue = infile.mapToPair(line ->{
            Double sValue;
            String sID;
            String[]parts = line.split(",");
            sValue = new Double(parts[2]);
            sID = parts[0];

            Tuple2<String, Double> pair = new Tuple2<String, Double>(sID, sValue);

            return pair;
        });

        JavaPairRDD<String, Double> MaxValue = SensorANDValue.reduceByKey((value1,value2) ->{
            if (value1 > value2)
                return value1;
            else 
                return value2;
        });

        MaxValue.saveAsTextFiel(outputpath);
        obj.close();
    }
}