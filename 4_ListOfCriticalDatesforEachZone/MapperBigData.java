package it.polito.bigdata.hadoop.exercise4;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData extends Mapper<
					Text,
					Text,
					Text,
					Text>{

	private static Double threshold = new Double(50);

	protected void map(
				Text key ,
				Text value,
				Context context)throws IOException,InterruptedException{

			String[] parts = key.toString().split(",");

			String zoneID = parts[0];
			String date = parts[1];
			Double sValue = new Double(value.toString());

			if(sValue > threshold){
				context.write(new Text(zoneID), new Text(date));
			}
		}
}