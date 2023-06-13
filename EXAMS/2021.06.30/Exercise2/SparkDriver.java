package it.polito.bigdata.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class SparkDriver{
    public static void main(String[] args) {
        
        String inputBike;
        String inputSales;
        String outputA;
        String outputB;

        inputBike = args[0];
        inputSales =args[1];
        outputA = args[2];
        outputB = args[3];

        SparkConf conf = new SparkConf().setAppName("2021.06.30");
        JavaSparkContext obj = new JavaSparkContext(conf);

        JavaRDD<String> inbike = obj.textFile(inputBike);
        JavaRDD<String> insales = obj.textFile(inputSales);

        JavaRDD<String> EUand2020 = insales.filter(line ->{
            String[] parts = line.split(",");
            String eu = parts[6];
            String date = parts[3];

            if(date.StratsWith("2020") && eu.equals("T")){
                return true;
            }else{
                return false;
            }
        });

        JavaPairRDD<String,Tuple2<Double,Double>> modelpricepair = EUand2020.mapToPair(line ->{ 
            String[] parts = line.split(",");
            String modelid = parts[2];
            Double price = Double.parseDoubel(parts[5]); 
            
            return new Tuple2<String,Tuple2<Double,Double>>(modelid,new Tuple2<Double,Double>(price,price));
        });

        JavaPairRDD<String,Tuple2<Double,Double>> minmax = modelpricepair.reduceByKey((v1,v2) ->{ 
            double min = Math.min(v1._1(), v2._1());
        	double max = Math.max(v1._2(), v2._2());
            return new Tuple2<Double,Double>(min, max);
        });

        JavaPairRDD<String,Double> variation = minmax.mapValues(pa -> pa._2()-pa._1());

        JavaPairRDD<String,Double> greater5000 = variation.filter(t -> t._2()>=5000);

        greater5000.keys().saveAsTextFile(outputA);

        //PartB

        
    }
}