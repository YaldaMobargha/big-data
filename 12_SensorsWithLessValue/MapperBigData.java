package it.polito.bigdata.hadoop.exercise12;

import java.io.IOException;

import javax.xml.soap.Text;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData extends Mapper<
							Text,
							Text,
							Text,
							FloatWritable>{

	protected void setup(Context context){
		thereshold = float.parseFloat(context.getConfiguration().get("maxThereshold"));
	}
								
	protected void map(
					Text key,
					Text value,
					Context context)throws IOException,InterruptedException{

		if(thereshold>sValue){
			context.write(new Text(key), new FloatWritable(sValue));
		}
	}
}