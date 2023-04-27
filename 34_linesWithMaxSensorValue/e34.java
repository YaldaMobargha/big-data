package it.polito.bigdata.spark.exercise34;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

public class SparkDriver {
    public static void main(String[] args){

        String inputpath;
        String outputpath;

        inputpath = args[0];
        outputpath = args[1];

        SparkConf conf = new SparkConf().setAppName("e34");

        JavaSparkContext obj = new JavaSparkContext(conf);

        JavaRDD<String> infile =  obj.textFile(inputpath);

        JavaRDD<Double> selectValues = infile.map(line -> {
            Double sValue;
            String[] parts = line.split(",");
            sValue = new Double(parts[2]);
            return sValue;
        });

        Double MaxValue = selectValues. reduce((value1, value2) ->{
            if (value1 > value2)
                return value1;
            else 
                return value2;
        });

        JavaRDD<String> linesWithMaxSensorValue = infile.filter(line ->{
            Double sValue;
            String[] parts = line.split(",");
            sValue = new Double(parts[2]);

            if (sValue.equals(MaxValue))
                return true;
            else 
                return false;
        });

        linesWithMaxSensorValue.saveAsTextFile(outputpath);
        obj.close();
    }
    
}
