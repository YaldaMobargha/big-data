package it.polito.bigdata.hadoop.exercise17;

import java.io.IOException;
import javax.swing.text.AbstractDocument.Content;
import javax.xml.soap.Text;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

class MapperType1BigData extends Mapper<
								LongWritable,
								Text,
								Text,
								FloatWritable>{

	protected void map(
				LongWritable key,
				Text value,
				Context context)throws IOException,InterruptedException{

		String[] parts = value.toString().split(",");
		String date = parts[1];
		Float temperature = Float.parseFloat(parts[3]);

		context.write(new Text(date), new FloatWritable(temperature));
	}
}