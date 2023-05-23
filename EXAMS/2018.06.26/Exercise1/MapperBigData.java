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
							IntWritable>{

	protected void map(
				LongWritable key, 
				Text value,
				Context context)throws IOException,InterruptedException{
		
		String[] parts = value.toString().split(",");
		String date = parts[0];
		String time = parts[1];
		String ID = parts[2];
		Float cpu = Float.parseFloat(parts[3]);

		String year = date.split("/")[0];
		String month = date.split("/")[1];
		int hour = Integer.parseInt(time.split(":")[0]);

		if(year.compareTo("2018")==0 && month.compareTo("05")==0 && hour>=9 && hour<18 && cpu>99.8){
			context.write(new Text(ID), new IntWritable(1));
		}
	}							
}