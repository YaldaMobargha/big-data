package it.polito.bigdata.spark.exercise36;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

public class SparkDriver{
    public static void main(String[] args){

        String inputPath;
        String outputPath;

        inputPath = args[0];
        outputPath = args[1];

        SparkConf conf = new SparkConf().setAppName("e36");
        JavaSparkContext obj = new JavaSparkContext(conf);

        JavaRDD infile = obj.textFile(inputPath);

        JavaRDD<String> selectValues = infile.map(line ->{
            Double sValue;

            String[]parts = line.split(",");
            sValue = new Double(parts[2]);
            return sValue;
        });

        Double sum = selectValues.reduce((Double value1,Double value2) -> new Double(value1+value2));
        //count number of lines(number of values of the sensor)
        long numlines = infile.count();

        Double avg = sum/numlines;

        system.out.println("Average:" + avg);
        obj.close();
    }
}