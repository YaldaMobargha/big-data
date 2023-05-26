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
				Context context)throws IOException,InterruptedException{

		String[] parts = value.toString().split(",");
		String sID = parts[2];
		String date = parts[0];
		String failureType = parts[3];

		if(date.startsWith("2016/04")==true && (failureType.compareTo("RAM")==0 || failureType.compareTo("hard_drive")==0)){
			context.write(new Text(sID), new Text(failureType));
		}
	}
}