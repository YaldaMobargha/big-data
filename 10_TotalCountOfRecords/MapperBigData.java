package it.polito.bigdata.hadoop.exercise10;

import it.polito.bigdata.hadoop.exercise10.DriverBigData.MY_COUNTERS;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData extends Mapper<
							LongWritable,
							Text,
							NullWritable,
							NullWritable>{

	protected void map(
				LongWritable key,
				Text value,
				Context context)throws IOException,InterruptedException{

		context.getCounter(MY_COUNTERS.TOTAL_RECORDS).increment(1);
	}
}
