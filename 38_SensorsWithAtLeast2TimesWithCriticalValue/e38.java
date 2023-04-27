package it.polito.bigdata.spark.exercise38;

import org.apache.spark.api.java.*;
import scala.Tuple2;
import org.apache.spark.SparkConf;

public class SparkDriver{
    public static void main(String[] args){

        String inputpath;
        String outputpath;

        inputpath = args[0];
        outputpath = args[1];

        SparkConf conf = new SparkConf().setAppName("e38");

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

        JavaPairRDD<String, Double> sensorpair = morethan50.mapToPair(line ->{
            Double sValue;
            String sID;

            String[] parts= line.split(",");
            sValue = new Double(parts[2]);
            sID = parts[0];

            Tuple2<String, Double> pair = new Tuple2<String, Double>(sID, 1);

            return pair;
        });

        JavaPairRDD<String, Double> countforeach = sensorpair.reduceByKey((value1, value2) -> new Double(value1+value2));

        JavaPairRDD<String, Double> checkmorethan2 = countforeach.filter((Tuple2<String, Double> countforeach) ->{
            if (countforeach._2().intValue()>=2)
                return true;
            else 
                return false;
        });

        checkmorethan2.saveAsTextFile(outputpath);
        obj.close();
    }
}