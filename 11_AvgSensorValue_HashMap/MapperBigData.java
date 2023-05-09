package it.polito.bigdata.hadoop.exercise11;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import javax.xml.soap.Text;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import it.polito.bigdata.spark.exercise43.Count;

class MapperBigData extends Mapper<
				LongWritable,
				Text, 
				Text, 
				SumCount> {

	HashMap<String, SumCount> records;

	protected void setup(Context context){
		records = new HashMap<String,SumCount>();
	}

	protected void map(
			LongWritable key,
			Text value, 
			Context context) throws IOException, InterruptedException {

		SumCount Stat;

		String[] parts = value.toString().split(",");
		String sID = fields[0];
		float sValue = Float.parseFloat(fields[2]);

		Stat = records.get(sID);

		if(Stat == null){
			Stat = new SumCount();
			Stat.setCount(CountValue:1);
			Stat.setSum(sValue);

			records.put(new String(sID),Stat);
		}else{
			Stat.setCount(Stat.getCount()+1);
			Stat.setSum(Stat.getSum()+ sValue);

			records.put(new String(sID),Stat);
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {

		for (Entry<String, SumCount> pair : statistics.entrySet()) {
			context.write(new Text(pair.getKey()), pair.getValue());
		}
	}				
}