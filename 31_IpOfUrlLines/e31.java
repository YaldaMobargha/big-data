package it.polito.bigdata.spark.exercise30;

import org.apache.spark.spi.java.*;
import org.apache.spark.SparkConf;

public class SparkDriver{
    public static void main(String[] args){

        String inputpath;
        String outputpath;

        inputpath = args[0];
        outputpath = args[1];

        SparkConf conf = new SparkConf().setAppName("e31");

        JavaSparkContext obj = new JavaSparkContext(conf);

        JavaRDD<String> infile = obj.textFile(inputpath);

        JavaRDD<String> includegoogle = infile.filter(t -> t.toLowerCase().contains("google"));

        JavaRDD<String> selectIP = includegoogle.map(line ->{
            String[] parts = line.split(" ");
            String ip = parts[0];

            return ip;
        });

        JavaRDD<String> IP = selectIP.distinct();
        IP.saveAsTextFile(outputpath);
        obj.close();

    }
}