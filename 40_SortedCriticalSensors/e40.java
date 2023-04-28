package it.polito.bigdata.spark.exercise40;

import org.apache.spark.api.java.*;
import scala.Tuple2;
import org.apache.spark.SparkConf;

public class SparkDriver{
    public static void main(String[] args){
         
        String inputpath;
		String outputpath;

		inputpath = args[0];
		outputpath = args[1];

        SparkConf conf = new SparkConf().setAppName("e40");

        JavaSparkContext obj = new JavaSparkContext(conf);

        JavaRDD<String> infile = textFile(inputpath);

        JavaRDD<String> morethan50 = infile.filter(line ->{
            Double sValue;
            String[] parts = line.split(",");
            sValue = new Double(parts[2]);

            if (sValue>50)
                return true;
            else
                return false;
        });

        JavaPairRDD <String,Integer> sensorpair = morethan50.mapToPair(line ->{
            String sID;
            String[] parts = line.split(",");
            sID = parts[0];
            Tuple2<String, String> pair = new Tuple2<String,Integer>(sID, 1); 
            return pair; 
        });

        JavaPairRDD <String,Integer> countforeach = sensorpair.reduceByKey((value1,value2) -> new Integer(value1+value2));

        JavaPairRDD <Integer, String> inverted = countforeach.mapToPair((Tuple2<String,Integer>p )-> new Tuple2<Integer,String>(p._2(),p._1()));

        JavaPairRDD <Integer, String> sorted = inverted.sortByKey(false);

        sorted.saveAsTextFile(outputpath);
        obj.close();
    }
}