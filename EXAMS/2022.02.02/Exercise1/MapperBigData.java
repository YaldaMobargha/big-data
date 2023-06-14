package it.polito.bigdata.hadoop;

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
		String category = parts[3];
		String price = parts[2];
		String company = parts[4];
		
		if(category.equals("Game")){
			if(price.equals("0")){
				context.write(new Text(company), new IntWritable(1));
			}else{
				context.write(new Text(company), new IntWritable(-1));
			}
		}
	}
}