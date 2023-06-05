package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;
import scala.Tuple2;
import java.util.ArrayList;
import org.apache.spark.SparkConf;

public class SparkDriver{
	public static void main(String[] args) {
		String inputPatches;
		String outputPathPartA;
		String outputPathPartB;

		inputPatches = args[0];
		outputPathPartA = args[1];
		outputPathPartB = args[2];

		SparkConf conf = new SparkConf().setAppName("2019.07.18");

		JavaSparkContext obj = new JavaSparkContext(conf);

		JavaRDD<String> infile = obj.textFile(inputPatches);

		JavaRDD<String> just2017 = infile.filter(line ->{
			String[] parts = line.split(",");
			String date = parts[1];
			String appname = parts[2];

			if (date.startsWith("2017") && (software.equals("Windows 10") || software.equals("Ubuntu 18.04")))
				return true;
			else
				return false;
		});

		JavaPairRDD<Integer, Counter> pair2017 = just2017.mapToPair(line -> {
			String[] fields = line.split(",");

			String software = fields[2];
			Integer month = Integer.parseInt(fields[1].split("/")[1]);

			if (software.equals("Windows 10"))
				return new Tuple2<Integer, Counter>(month, new Counter(1, 0));
			else // Ubuntu 18.04
				return new Tuple2<Integer, Counter>(month, new Counter(0, 1));
		});

		JavaPairRDD<Integer, Counter> sumpairs = pair2017.reduceByKey((v1,v2) -> new Counter(v1.getNumPatchesWindows() + v2.getNumPatchesWindows(),
		v1.getNumPatchesUbuntu() + v2.getNumPatchesUbuntu()));

		JavaPairRDD<Integer, Counter> noteven = sumpairs.filter(pair -> {
			if (pair._2().getNumPatchesWindows() != pair._2().getNumPatchesUbuntu())
				return true;
			else
				return false;
		});

		JavaPairRDD<Integer, String> greater = noteven.mapToPair(pair -> {
			if (pair._2().getNumPatchesUbuntu()>pair._2().getNumPatchesWindows())
				return new Tuple2<Integer, String>(pair._1(), "U");
			else 
				return new Tuple2<Integer, String>(pair._1(), "W");
		});

		greater.saveAsTextFile(outputPathPartA);

		//partB
		JavaRDD<String> year2018 = infile.filter(line ->{
			String[] parts = line.split(",");
			String date = parts[1];

			if (date.startsWith("2018"))
				return true;
			else
				return false;
		});
	
		JavaPairRDD<MonthSoftware, Integer> pair2018 = year2018.mapToPair(line -> {
			String[] parts = line.split(",");
			String appname = parts[2];
			String month =parts[1].split("/")[1];

			return new Tuple2<MonthSoftware, Integer>(new MonthSoftware(month,appname), 1);
		});

		JavaPairRDD<MonthSoftware, Integer> summonthAppname = pair2018.reduceByKey((value1, value2) -> new Integer(value1+value2));

		JavaPairRDD<MonthSoftware, Integer> greaterthan4 = summonthAppname.filter(pair ->{
			if (pair._2()>=4)
				return true;
			else
				return false;
		});

		JavaPairRDD<MonthSoftware, Integer> windowcreation = greaterthan4.flatMapToPair(pair ->{
			ArrayList<Tuple2<MonthSoftware,Integer>> elements = new ArrayList<Tuple2<MonthSoftware,Integer>>;

			MonthSoftware current = pair._1();
			int currentmonth = current.getMonth();
			int currentsoftware = current.getSoftware();

			elements.add(new Tuple2<MonthSoftware, Integer>(new MonthSoftware(currentmonth,currentsoftware), 1));
			if (currentMonth - 1 > 0)
			elements.add(new Tuple2<MonthSoftware, Integer>(new MonthSoftware(currentMonth - 1, currentSoftware), 1));
			if (currentMonth - 2 > 0)
			elements.add(new Tuple2<MonthSoftware, Integer>(new MonthSoftware(currentMonth - 2, currentSoftware), 1));

		return elements.iterator();
		});

		JavaPairRDD<MonthSoftware, Integer> numwindow = windowcreation.reduceByKey((v1, v2) -> v1 + v2);

		JavaPairRDD<MonthSoftware, Integer> equal3= numwindow.filter(pair ->{
			if(pair._2()==3){
				return true;
			}else{
				return false;
			}
		});

		equal3.keys().saveAsTextFile(outputPathPartB);

		JavaRDD<String> selectedSoftwares = selectedWindowsSoftware.map(pair -> pair._1().getSoftware()).distinct();
		
		System.out.println(selectedSoftwares.count());
		
		obj.close();
	}
}