package it.polito.bigdata.hadoop.exercise17;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

class MapperType2BigData extends Mapper<
								LongWritable,
								Text,
								Text,
								FloatWritable>{

	protected void map(
				LongWritable key,
				Text value,
				Context context)throws IOException,InterruptedException{

		String[] parts = value.toString().split(",");
		String date = parts[0];
		Float temperature = Float.parseFloat(parts[2]);

		context.write(new Text(date), new FloatWritable(temperature));
	}
}