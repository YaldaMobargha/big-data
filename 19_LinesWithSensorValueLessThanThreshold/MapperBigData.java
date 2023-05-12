package it.polito.bigdata.hadoop.exercise20;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import javafx.scene.text.Text;

class MapperBigData extends Mapper<
							LongWritable,
							Text,
							Text,
							NullWritable>{

	private static Float Thereshold = new Float(30);
	
	protected void map(
				LongWritable key, 
				Text value, 
				Context context)throws IOException, InterruptedException{

		String[] parts = value.toString().split(",");
		Float temperature = Float.parseFloat(parts[3]);

		if(temperature<= Thereshold){
			context.write(new Text(value),NullWritable.get());
		}
	}
}
					