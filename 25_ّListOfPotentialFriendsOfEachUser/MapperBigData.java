package it.polito.bigdata.hadoop.exercise25;

import java.io.IOException;
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
		
                context.write(new Text(parts[0]),new Text(parts[1]));
		context.write(new Text(parts[1]),new Text(parts[0]));

	}
}