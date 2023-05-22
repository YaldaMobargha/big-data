package it.polito.bigdata.hadoop.exercise1;

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
					Context context)throws IOException,InterruptedException{
		
		String[] parts = value.toString().split(",");
		String city = parts[1];
		Float min = Float.parseFloat(parts[4]);
		Float max = Float.parseFloat(parts[3]);

		if(max>35){
			context.write(new Text(city), new Text("Greater"));
		}

		if(min<-20){
			context.write(new Text(city), new Text("Less"));
		}

	}
}