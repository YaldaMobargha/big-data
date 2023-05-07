package it.polito.bigdata.hadoop.exercise6;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import javafx.scene.text.Text;

class MapperBigData extends Mapper<
					LongWritable,
					Text,
					Text,
					FloatWritable>{

	protected void map(
				LongWritable key,
				Text value,
	 			Context context)throws IOException,InterruptedException{
		
		String[] parts = value.toString().split(",");
		String sID = parts[0];
		Float sValue = float.parseFloat(parts[2]);

		context.write(new Text(sID), new FloatWritable(sValue));
	}
}