package it.polito.bigdata.hadoop.exercise20;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

class MapperBigData extends Mapper<
								LongWritable,
								Text,
								Text,
								NullWritable>{

	private static float Threshold = new Float(30);

	protected void map(
				LongWritable key,
				Text value,
				Context context)throws IOException,InterruptedException{

		String[] parts = value.toString().split(",");
		
		Float temperature = Float.parseFloat(parts[3]);
		if(temperature>Threshold){
			context.write(value, NullWritable.get());
		}
	}
}