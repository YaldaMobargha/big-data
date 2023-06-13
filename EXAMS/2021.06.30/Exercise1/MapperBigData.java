package it.polito.bigdata.hadoop;

import java.io.IOException;

import javax.xml.transform.Templates;

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
		String modelid = parts[2];
		String date = parts[3];
		String year = date.split("/")[0];
		String eu = parts[6];

		if(year.equals("2020")){
			context.write(new Text(modelid), new Text(eu));
		}
	}
}