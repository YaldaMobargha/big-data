package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData extends Mapper<
							LongWritable,
							Text,
							Text,
							Text>{
	protected void map(
				LongWritable key, 
				Text value, 
				Context context)throws IOException, InterruptedException{

		String[] parts = value.toString().split(",");
		String airport = parts[5];
		String cancelled = parts[8];
		String date = parts[2];

		if(date.compareTo("2016/09/01")>=0 && date.compareTo("2017/08/31")<=0){
			context.write(new Text(airport), new Text(cancelled));
		}
	}
}