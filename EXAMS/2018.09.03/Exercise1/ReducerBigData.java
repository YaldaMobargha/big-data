package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData extends Reducer<
							NullWritable,
							Text,
							DoubleWritable,
							Text>{
	
	@Override
	protected void reduce(
				NullWritable key, 
				Iterable<Text> values, 
				Context context)throws IOException,InterruptedException{

		double glohighest= Double.MIN_VALUE;
		String glofirstTime = null;
		
		for(Text value: values){

			String[] parts = value.toString().split(",");
			String timeStamp = parts[1];
			Double price = double.parseDouble(parts[0]);
			
			if(price>=glohighest){
				glohighest = price;

				String timeStamp = new String(date+","+time);

				if(timeStamp.compareTo(glofirstTime)<=0 || glofirstTime==null){
					glofirstTime=timeStamp;
				}
			}
		}
		context.write(new DoubleWritable(glohighest) ,new Text(highest+","+firstTime));
	}
}

			
