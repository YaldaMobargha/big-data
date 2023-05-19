package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData extends Mapper<
							LongWritable,
							Text,
							Text,
							FloatWritable>{

	protected void map(
				LongWritable key, 
				Text value, 
				Context context)throws IOException, InterruptedException{
		
		String[] parts = value.toString().split(",");
		String stockId = parts[0];
		String date = parts[1];
		String year = date.split("/")[0];
		String month = date.split("/")[1];
		Float price = Float.parseFloat(parts[3]);

		if(year.compareTo("2016")==0){
			context.write(new Text(stockId+"-"+month), new FloatWritable(price));
		}
	}
}