package it.polito.bigdata.spark.exercise46;

import org.apache.spark.api.java.*;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.spark.SparkConf;

public class SparkDriver {
    @SuppressWarnings("resource")

    public static void main(String[]args) {

        String inputpath;
        String outputpath;

        inputpath = args[0];
        outputpath = args[1];

        SparkConf conf = new SparkConf().setAppName("e46");

        JavaSparkContext obj = new JavaSparkContext(conf);

        JavaRDD <String> infile = obj.textFile(inputpath);

        JavaPairRDD<Integer, TST> WindowRDD = infile.flatMapToPair(line ->{
            Integer time;
            Double temp;
            ArrayList<Tuple2<Integer,TST>> pairs = new ArrayList<Tuple2<Integer,TST>>();
            
            String[] parts = line.split(",");
            time = Integer.parseInt(parts[0]);
            temp = Double.parseDouble(fields[1]);

            Tuple2<Integer,TST> pair = new Tuple2<Integer,TST>(time,new TST(time,temp));
            pairs.add(pair);

            Tuple2<Integer, TST> pair = new Tuple2<Integer,TST>(time-60, new TST(time,temp));
            pairs.add(pair);

            Tuple2<Integer,TST> pair = new Tuple2<String,TST> (time-120, new TST(time,temp));
            pairs.add(pair);

            return pairs.iterator();
        });

        JavaPairRDD <Integer, Iterable<TST>> timestampwindow = WindowRDD.groupByKey();

        JavaRDD<Iterable<TST>> windows = timestampwindow.values();

        JavaRDD<Iterable<TimeStampTemperature>> seletedWindows = windows.filter((Iterable<TimeStampTemperature> listElements) -> {
            HashMap<Integer, Double> timetemphash = new HashMap<Integer,Double>();

            int minTime = Integer.MAX_VALUE;
            boolean increasing;

            for(TST element : listElements){
                timetemphash.put(element.getTimestamp(),element.getTemprature());
                if (element.getTimestamp() < minTime){
                    minTime = element.getTimestamp();
                }
            }

            if (timetemphash.size() == 3){
                increasing = true;

                for(int t = minTime + 60; t <= minTime + 120 && increasing == true; t = t+60){
                    if (timetemphash.get(t) <= timetemphash.get(t-60)){
                        increasing = false;
                    }
                }
            } else {
                increasing = false;
            }

            return increasing;
        });

        seletedWindows.saveAsTextFile(outputpath);
        obj.close();

    }
}
