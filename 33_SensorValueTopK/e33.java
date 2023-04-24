package it.polito.bigdata.spark.exercise30;

import org.apache.spark.spi.java.*;
import org.apache.spark.SparkConf;

public class SparkDriver{
    public static void main(String[] args){

        String inputPath;
        String outputPath;

        inputPath = args[0];
        outputPath = args[1];

        SparkConf conf = new SparkConf().setAppName("e32");

        JavaSparkContext obj = new JavaSparkContext(conf);

        JavaRDD<String> infile = obj.textFile(inputPath);

        JavaRDD<Double> selectValues = infile.map(line ->{
            Double sValue;

            String[] parts = line.split(",");
            sValue = new Double(parts[2]);
            return sValue;
        });

        //1st solution
        List<Double> top3values = selectValues.top(3);

        for (Double value : top3values){
            system.out.println(value);
        }

        //2nd solution
        //??????

        obj.close();
    }
}