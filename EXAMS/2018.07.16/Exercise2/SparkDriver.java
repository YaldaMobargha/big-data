package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import javafx.geometry.Side;
import javafx.util.Pair;
import scala.Tuple2;
import org.apache.spark.SparkConf;

public class SparkDriver{

	public static void main(String[] args) {
		
		String inputpathservers;
		String inputpathfailures;
		String outputpathA;
		String outputpathB;

		inputpathservers = args[0];
		inputpathfailures = args[1];
		outputpathA = args[2];
		outputpathB = args[3];

		SparkConf conf = new SparkConf().setAppName("2018.07.16");

		JavaSparkContext obj = new JavaSparkContext(conf);

		JavaRDD<String> servers = obj.textFile(inputpathservers);

		JavaRDD<String> failures = obj.textFile(inputpathfailures);

		JavaRDD<String> fail2017= failures.filter(line ->{
			String[] parts = line.split(",");
			String date = parts[0];

			if(date.startsWith("2017")==true){
				return true;
			}else{
				return false;
			}
		}).cache();

		JavaPairRDD<String, Integer> numfail= fail2017.mapToPair(line ->{
			String[] parts = line.split(",");
			String sID = parts[2];

			return new Tuple2<String, Integer>(sID, 1);
		});

		JavaPairRDD<String, Integer> sum= numfail.reduceByKey((value1, value2) -> value1, value2);

		JavaPairRDD<String, String> pairserver= servers.mapToPair(line ->{
			String[] parts = line.split(",");
			String sID = parts[0];
			String DataCenterID = parts[2];

			return new Tuple2<String, String>(sID, DataCenterID);
		});

		JavaPairRDD<String, Tuple2<String,Integer>> joinedpair = pairserver.join(sum);

		JavaPairRDD<String, Integer> centercount = joinedpair.mapToPair(pair -> new Tuple2<String, Integer>(pair._2()._1(), pair._2()._2()));

		JavaPairRDD<String, Integer> sumcenter = centercount.reduceByKey((v1, v2) -> v1 + v2);

		JavaPairRDD<String, Integer> greater365 = sumcenter.filter(t -> {
			if (t._2() >= 365)
				return true;
			else
				return false;
		});

		greater365.saveAsTextFile(outputpathA);

		//partB
		

	}
}