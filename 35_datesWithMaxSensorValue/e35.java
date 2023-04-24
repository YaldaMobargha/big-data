package it.polito.bigdata.spark.exercise30;

import org.apache.spark.spi.java.*;
import org.apache.spark.SparkConf;

public class SparkDriver{
    public static void main(String[] args){

        String inputPath;
        String outputPath;

        inputPath = args[0];
        outputPath = args[1];

        SparkConf conf = new SparkConf().setAppName("e35");

        JavaSparkContext obj = new JavaSparkContext(conf);

        JavaRDD<String> infile = obj.textFile(inputPath);

        JavaRDD<String> selectdate = infile.map(line ->{
            Double sValue;

            String[] parts = line.split(",");
            sValue = parts[2];
            return sValue;
        });

        Double MaxValue = selectdate.reduce((value1,value2)->{
            if(value1 > value2)
                return value1;
            else 
                return value2;
        });

        JavaRDD<String> linesWithMaxSensorValue = infile.filter(line ->{
            Double sValue;
            
            String[]parts = line.split(",");
            sValue = parts[2];

            if(sValue.equals(MaxValue))
                return true;
            else
                return false;
        });

        JavaRDD <String> dates = linesWithMaxSensorValue.map(line ->{
            String[]parts = line.split(",");
            date= parts[1];
            return date;
        });

        JavaRDD<String> disdate = dates.distinct();

        disdate.saveAsTextFiel(outputPath);
        obj.close();
    }
}