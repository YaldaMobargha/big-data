package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;
import scala.Tuple2;

import javax.xml.crypto.dsig.spec.XPathType.Filter;

import org.apache.spark.SparkConf;

public class SparkDriver{
	public static void main(String[] args){

		String inputpathFlights;
		String inputpathAirports;
		String outputpathA;
		String outputpathB;

		inputpathFlights = args[0];
		inputpathAirports = args[1];
		outputpathA = args[2];
		outputpathB = args[3];

		SparkConf conf = new SparkConf().setAppName("2017.09.14");

		JavaSparkContext obj = new JavaSparkContext(conf);

		//partA

		JavaRDD<String> airports = obj.textFile(inputpathFlights);
		JavaRDD<String> flights = obj.textFile(inputpathFlights);

		JavaRDD<String> germanyairports = airports.Filter(line ->{
			String[] parts = line.split(",");
			String country = parts[3];

			if(country.compareTo("Germany")==0){
				return true;
			}else{
				return false;
			}
		});

		JavaPairRDD<String,String> pairgermanyairports = germanyairports.mapToPair(line ->{
			String[] parts = line.split(",");
			String aID = parts[0];
			String name = parts[1];

			Tuple2<String,String> pair = new Tuple2<String,String>(aID,name);
				return pair;
		});

		JavaRDD<String> arrival = flights.Filter(line ->{
			String[] parts = line.split(",");
			Float delay = Float.parseFloat(parts[7]);

			if(Double delay>=15){
				return true;
			}else{
				return false;
			}
		});

		JavaPairRDD<String,String> pairarrival = arrival.mapToPair(line ->{
			String[] parts = line.split(",");
			String airline = parts[1];
			String arrivalairportID = parts[6];

			Tuple2<String,String> pair1 = new Tuple2<String,String>(arrivalairportID, airline);
				return pair1;
		});
		//key:Id.  value:airline, name//
		JavaPairRDD<String,Tuple2<String,String> joined = pairarrival.join(pairgermanyairports);

		JavaPairRDD<String,Integer> numairport = joined.mapToPair((Tuple2<String,Tuple2<String, String>> joinedpair)->{
			String Airline = joinedpair._2._1();
			String airname = joinedpair._2._1();

			Tuple2<String,Integer> pair2 = new Tuple2<String,Integer>(Airline+","+airname, new Integer(1));
				return pair2;
		});

		JavaPairRDD<String,Integer> count = numairport.reduceByKey((value1, value2)-> new Integer(value1+value2));

		JavaPairRDD<Integer,String> pairSwaped = count.mapToPair((Tuple2<String,Integer> t )->{

			Tuple2<Integer,String> pair3 = new Tuple2<Integer,String>(new Integer(t._2()), new String(t._1()));
				return pair3;
		});

		JavaPairRDD<Integer,String> sorted = pairSwaped.sortByKey(false);

		sorted.saveAsTextFile(outputpathA);

		//partB
		JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> fullyBookedOrCancelledOnes = flights.mapToPair(line -> {
			
			String[] parts = line.split(",");
			String route = parts[5] + "," + parts[6];
			String cancelled = parts[8];
			int num_of_seats = Integer.parseInt(parts[9]);
			int num_of_booked_seats = Integer.parseInt(parts[10]);
		
			if(num_of_seats==num_of_booked_seats){
				numFullyBooked=1;
			}
			if(cancelled.compareTo("yes")==0){
				numCancelled=1;
			}
			int numFlights = 1;
		
			return new Tuple2<>(route, new Tuple3<>(numFullyBooked, numCancelled, numFlights));
		});

		JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> summedCounts = fullyBookedOrCancelledOnes.reduceByKey((count1, count2) ->
        new Tuple3<>(count1._1 + count2._1, count1._2 + count2._2, count1._3 + count2._3));

		JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> result = summedCounts.filter( (Tuple2<String, Tuple3<Integer, Integer, Integer>> tuptup) ->{
			double percFullyBooked = (double) tuptup._2._1 / (double) tuptup._2._3;
			double percCancelled = (double) tuptup._2._2 / (double) tuptup._2._3;
	
			if (percFullyBooked >= 0.99 && percCancelled >= 0.05)
				return true;
			else
				return false;
		});

		result.keys.saveAsTextFile(outputpathB);

		obj.close();
	}
}