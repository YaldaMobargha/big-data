package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData extends Mapper<
							LongWritable,
							Text,
							Text,
							IntWritable>{
	protected void map(
				LongWritable key, 
				Text value, 
				Context context)throws IOException,InterruptedException{
		
		String[] parts = value.toString().split(",");
		String date = parts[1];
		String appname = parts[2];

		if (date.startsWith("2018")){
			context.write(new Text(appname), new IntWritable(1));
		}
		if (date.startsWith("2017")){
			context.write(new Text(appname), new IntWritable(-1));
		}
	}
}