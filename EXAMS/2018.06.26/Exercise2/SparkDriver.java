package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;
import scala.Tuple2;
import org.apache.spark.SparkConf;

public class SparkDriver{
	public static void main(String[] args) {
		
		String inputpath;
		String outputpathA;
		String outputpathB;
		double cputhereshold;
		double ramthereshold;

		CPUthr = Double.parseDouble(args[0]);
		RAMthr = Double.parseDouble(args[1]);
		inputpath = args[2];
		outputpathA = args[3];
		outputpathB = args[4];

		SparkConf conf = new SparkConf().setAppName("2018.06.26");

		JavaSparkContext obj = new JavaSparkContext(conf);

		JavaRDD<String> infile = obj.textFile(inputpath);
		
		JavaRDD<String> may2018 = infile.filter(line ->{
			String[] parts = line.split(",");
			String date = parts[0];
			
			if(date.startsWith("2018/05")==true){
				return true;
			}else{
				return false;
			}
		}).cache();

		JavaPairRDD<String,Counter> pairmay2018 = may2018.mapToPair(line ->{
			String[] parts = line.split(",");
			String ID = parts[2];
			String hour = (parts[1]).split(":")[0];
			Double cpu = Double.parseDouble(parts[3]);
			Double Ram = Double.parseDouble(parts[4]);

			Counter cou = new Counter(1, cpu, Ram);
			
			return new Tuple2<String,Counter>(new String(ID+"_"+hour), cou);
		});

		JavaPairRDD<String,Counter> avg = pairmay2018.reduceByKey((Counter c1, Counter c2) -> new Counter(c1.getCount()+c2.getCount(), c1.getSumCPUutil() + c2.getSumCPUutil(), c1.getSumRAMutil() + c2.getSumRAMutil()));

		JavaPairRDD<String,Counter> selected = avg.filter(pair->{

			double avgcpu = pair._2().getSumCPUutil() / (double) pair._2().getCount();
			double avgram = pair._2().getSumRAMutil() / (double) pair._2().getCount();
			if(avgcpu>cputhereshold && avgram>ramthereshold){
				return true;
			}else{
				return false;
			}
		});

		selected.keys().saveAsTextFile(outputpathA);

		//partB
		JavaPairRDD<String,Double> paircpuusage = may2018.mapToPair(line ->{
			String[] parts = line.split(",");
			String ID = parts[2];
			String hour = (parts[1]).split(":")[0];
			String date = parts[0];
			Double cpu = Double.parseDouble(parts[3]);
			
			return new Tuple2<String,Counter>(new String(ID+"_"+date+"_"+hour), cpu);
		});

		JavaPairRDD<String,Double> maxcpuusage = paircpuusage.reduceByKey((value1,value2) ->{
			if(value1>value2){
				return value1;
			}else{
				return value2;
			}
		});

		JavaPairRDD<String,Double> greaterthan = maxcpuusage.filter(pair ->{
			if(pair._2()>90 || pair._2()<10){
				return true;
			}else{
				return false;
			}
		});

		JavaPairRDD<String, CounterHighLowCPU> pairgreaterthan = greaterthan.mapToPair(pair -> {
			String[] parts = pair._1().split("_");
			String Id = parts[0];
			String date = parts[1];

			Double maxCPUUtil = pair._2();
			if (maxCPUUtil > 90.0){
				return new Tuple2<String, CounterHighLowCPU>(new String(Id + "_" + date),new CounterHighLowCPU(1, 0));
			}else{
				return new Tuple2<String, CounterHighLowCPU>(new String(Id + "_" + date),new CounterHighLowCPU(0, 1));
			}
		});

		JavaPairRDD<String, CounterHighLowCPU> sumgreaterthan = pairgreaterthan.reduceByKey((CounterHighLowCPU c1, CounterHighLowCPU c2) -> new CounterHighLowCPU(c1.getHighCPU() + c2.getHighCPU(),
					c1.getLowCPU() + c2.getLowCPU()));

		JavaPairRDD<String, CounterHighLowCPU> enough = sumgreaterthan.filter(pair -> {
			CounterHighLowCPU HighLow = pair._2();
			if (HighLow.getHighCPU() >= 8 && HighLow.getLowCPU() >= 8)
				return true;
			else
				return false;
		});

		enough.saveAsTextFile(outputpathB);
		obj.close();
	}
}