package it.polito.bigdata.spark.exercise30;

import org.apache.spark.spi.java.*;
import org.apache.spark.SparkConf;

public class SparkDriver{
   
    public static void main(String[] args){
         
         String inputPath;
         String outputPath;

         inputPath = args[0];
         outputPath = args[1];

         SparkConf conf = new SparkConf().setAppName("Spark Exercise #30")

         JavaSparkContext obj = new JavaSparkContext(conf);

         JavaRDD<String> infile = obj.textFile(inputPath);

         JavaRDD<String> includegoogle = infile.filter(t -> t.toLowerCase().contains("google"));

         includegoogle.saveAsTextFile(outputPath);

         obj.close();

    }
}