package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData extends Reducer<
							Text,
							FloatWritable,
							Text,
							Text>{

	protected void reduce(
				Text key, 
				Iterable<FloatWritable> values, 
				Context context)throws IOException, InterruptedException{
		
		Float min = Float.MAX_VALUE;
		Float max = Float.MIN_VALUE;

		for(String value: values){
			if(value.get()>max){
				max = value.get();
			}
			if(value.get()<min){
				min = value.get();
			}
		}

		Float dif = max-min;
		Float percentage = 100*(max-min)/min;

		if(percentage >5){
			context.write(new Text(key), new Text(dif+","+percentage));
		}
	}
}