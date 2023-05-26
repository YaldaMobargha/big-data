package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;
import scala.Tuple2;
import java.util.ArrayList;
import org.apache.spark.SparkConf;

public class SparkDriver{
	public static void main(String[] args) {
		String inputpath;
		String outputpathA;
		String outputpathB;

		inputpath = args[0];
		outputpathA = args[1];
		outputpathB = args[2];

		SparkConf conf = new SparkConf().setAppName("2018.09.03");

		JavaSparkContext obj = new JavaSparkContext(conf);

		JavaRDD<String> infile = obj.textFile(inputpath);

		JavaRDD<String> stock2017= infile.filter(line ->{
			String[] parts = line.split(",");
			String date = parts[1];

			if(date.startsWith("2017")==true){
				return true;
			}else{
				return false;
			}
		});

		JavaPairRDD<String, MinMaxPrices> pairstockdate= stock2017.mapToPair(line ->{
			String[] parts = line.split(",");
			String sID = parts[0];
			String date = parts[1];
			Double price = Double.parseDouble(parts[3]);

			MinMaxPrices va = new MinMaxPrices(price,price);

			return new Tuple2<String, MinMaxPrices>(new String(sID+"_"+date), va);
		});

		JavaPairRDD<String, MinMaxPrices> mm = pairstockdate.reduceByKey((v1, v2) -> {

			double minP;
			double maxP;

			if (v1.getMinPrice() < v2.getMinPrice())
				minP = v1.getMinPrice();
			else
				minP = v2.getMinPrice();

			if (v1.getMaxPrice() > v2.getMaxPrice())
				maxP = v1.getMaxPrice();
			else
				maxP = v2.getMaxPrice();

			MinMaxPrices value = new MinMaxPrices(minP, maxP);

			return value;
		});

		JavaPairRDD<String,Double> difference = mm.mapValues(minmaxP -> new Double(minmaxP.getMaxPrice()-minmaxP.getMinPrice())).cache();

		JavaPairRDD<String,Double> greaterthan10 = difference.filter(pair -> {
			if(pair._2()>10){
				return true;
			}else{
				return false;
			}
		});
		
		JavaPairRDD<String, Integer> numdate= greaterthan10.mapToPair(pp ->{
			String[] parts = pp._1().split(",");
			String sID = parts[0];

			return new Tuple2<String, Integer>(new String(sID), 1);
		});

		JavaPairRDD<String, Integer> SUMnumdate= numdate.reduceByKey((value1,value2) ->new Integer(value1+value2));

		SUMnumdate.saveAsTextFile(outputpathA);

		//partB

		JavaPairRDD<StockDate,Double> twodayspair = difference.flatMapToPair(daily ->{
			String[] parts = pp._1().split(",");
			String sID = parts[0];
			String date = parts[1];

			String previousdate = DateTool.previousDate(date);
			
			Double dailyVariation = new Double(daily._2());

			ArrayList<Tuple2<StockDate, Double>> groupdatearray = new ArrayList<Tuple2<StockDate, Double>>();

			StockDate IdDate = new StockDate(sID,date);
			StockDate IdDatePrevious = new StockDate(sID, previousdate);

			groupdatearray.add(new Tuple2<StockDate,Double>(IdDate,dailyVariation));
			groupdatearray.add(new Tuple2<StockDate,Double>(IdDatePrevious,dailyVariation));

			return groupdatearray.iterator();
		});

		JavaPairRDD<StockDate,iterator<Double>> twoGP= twodayspair.groupByKey();

		JavaPairRDD<StockDate,iterator<Double>> stable = twoGP.filter(tt ->{
			Double dailyV1 = null;
			Double dailyV2 = null;

			for(Double value : tt._2()){
				if (dailyV1 == null) { 
					dailyV1 = new Double(value);
				} else {
					dailyV2 = new Double(value);
				}
			}

			if (dailyV2 != null && Math.abs(dailyV1 - dailyV2) <= 0.1)
						return true;
					else
						return false;
		});

		stable.saveAsTextFile(outputpathB);

		obj.close();
	}
}