package it.polito.bigdata.spark.exercise43;

import org.apache.spark.api.java.*;
import org.apache.spark.broadcast.Broadcast;

import javafx.scene.shape.Line;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.spark.SparkConf;

public class SparkDriver{
    public static void main(String[] args){

        String inputpathReadings;
        String inputpathNeighbors;
        String outputpath1;
        String outputpath2;
        String outputpath3;
        
        int threshold;

        inputpathReadings = args[0];
        inputpathNeighbors = args[1];
        outputpath1 = args[2];
        outputpath2 = args[3];
        outputpath3 = args[4];

        threshold = Integer.parseInt(args[5]);

        SparkConf conf = new SparkConf().setAppName("e43");
        JavaSparkContext obj = new JavaSparkContext(conf);

        JavaRDD<String> infileReadings = obj.textFile(inputpathReadings).cache();
        JavaRDD<String> infileNeighbors = obj.textFile(inputpathNeighbors);

        //part_1
        JavaPairRDD<String,Count> CriticalSituation = infileReadings.mapToPair(line ->{
            String sID;
            Integer freeSlots;
            String[] parts = line.split(",");
            sID = parts[0];
            freeSlots = Integer.parseInt(parts[5]);

            if (freeSlots < threshold)
				return new Tuple2<String, Count>(sID, new Count(1, 1));
			else
				return new Tuple2<String, Count>(sID, new Count(1, 0));
        });

        JavaPairRDD<String,Count> countfree = CriticalSituation.reduceByKey(line ->{
            return new Tuple2<String, Count>(sID, new Count(value1, 1));
        })

    }
}
