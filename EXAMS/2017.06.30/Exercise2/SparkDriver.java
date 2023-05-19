package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

public class SparkDriver{
	public static void main(String[] args){
		String inputpath;
		String outputpathA;
		String outputpathB;
		int nw;

		inputpath = args[0];
		nw = Integer.parseInt(args[1]);
		outputpathA = args[2];
		outputpathB = args[3];

		SparkConf conf = new SparkConf().setAppName("2017.06.30");

		JavaSparkContext obj = new JavaSparkContext(conf);

		//partA
		JavaRDD<String> prices = obj.textFile(inputpath);
		 
		JavaRDD<String> p2016 = prices.filter(line ->{
			String[] parts = line.split(",");
			String date = parts[1];
			String year = date.split("/")[0];

			if(year.compareTo("2016")==0){
				return true;
			}else{
				return false;
			} 
		});

		JavaPairRDD<String,Double> stockIdPair = p2016.mapToPair(line ->{
			String[] parts = line.split(",");
			String date = parts[1];
			Double price = new Double(parts[3]);

			Tuple2<String,Double> pair = new Tuple2<String,Double>(stockId +"_"+ date, new Double(price));
			return pair;
		}).cache();

		JavaPairRDD<String, Double> lowest = stockIdPair.reduceByKey((price1, price2) -> {
			if (price1.doubleValue() < price2.doubleValue()) {
				return new Double(price1);
			} else {
				return new Double(price2);
			}
		});

		JavaPairRDD<String, Double> sorted = lowest.sortByKey();

		sorted.saveAsTextFile(outputpathA);

		//partB

		JavaPairRDD<String, Double> highest = stockIdPair.reduceByKey((price1, price2) -> {
			if (price1.doubleValue() > price2.doubleValue()) {
				return new Double(price1);
			} else {
				return new Double(price2);
			}
		});

		JavaPairRDD<String, FirstDatePriceLastDatePrice> week = stockIdPair.mapToPair((Tuple2<String, Double> input) -> {
			String[] keys= input._1().split("_");
			String stockId = keys[0];
			String date = keys[1];
			Integer weekNum = DateTool.weekNumber(date);
			Double price = input._2();
			FirstDatePriceLastDatePrice dp = new FirstDatePriceLastDatePrice(date, price, date, price);

			Tuple2<String, FirstDatePriceLastDatePrice> pair2 = new Tuple2<String, FirstDatePriceLastDatePrice>(stockId + "_" + weekNum, dp);
			return pair2;
		});

		JavaPairRDD<String, FirstDatePriceLastDatePrice> minmaxweek = week.reduceByKey((FirstDatePriceLastDatePrice dp1, FirstDatePriceLastDatePrice dp2) -> {
			String firstdate;
			Double firstprice;
			String lastdate;
			Double lastprice;

			if (dp1.firstdate.compareTo(dp2.firstdate) < 0){
				firstdate = dp1.firstdate;
				firstprice = dp1.firstprice;
			}else{
				firstdate = dp2.firstdate;
				firstprice = dp2.firstprice;
			}

			if (dp1.lastdate.compareTo(dp2.lastdate) > 0) {
				lastdate = dp1.lastdate;
				lastprice = dp1.lastprice;
			} else {
				lastdate = dp2.lastdate;
				lastprice = dp2.lastprice;
			}

			return new FirstDatePriceLastDatePrice(firstdate, firstprice, lastdate, lastprice);
		});

		JavaPairRDD<String, FirstDatePriceLastDatePrice> positive = minmaxweek.filter((Tuple2<String, FirstDatePriceLastDatePrice> inputPair) -> {
			if (inputPair._2().lastprice.doubleValue() > inputPair._2().firstprice.doubleValue()) {
				return true;
			} else {
				return false;
			}
		});

		JavaPairRDD<String, Integer> mapPositive = positive.mapToPair((Tuple2<String, FirstDatePriceLastDatePrice> inputPair) -> {
			String fields[] = inputPair.split("_");
			String stockId = fields[0];

			Tuple2<String, Integer> pair3 = new Tuple2<String, Integer>(new String(stockId), new Integer(1));
			return pair3;
		});

		JavaPairRDD<String, Integer> numpositive = mapPositive.reduceByKey((value1, value2) -> new Integer(value1 + value2));

		JavaPairRDD<String, Integer> thereshold = numpositive.filter((Tuple2<String, Integer> inputPair) -> {
					if (inputPair._2().compareTo(nw) >= 0) {
						return true;
					} else {
						return false;
					}
		});

		thereshold.keys().saveAsTextFile(outputPathPartB);

		obj.close();
	}
}