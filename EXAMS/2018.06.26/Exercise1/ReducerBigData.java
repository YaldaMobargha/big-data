package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData extends Reducer<
							Text,
							IntWritable,
							Text,
							NullWritable>{

	@Override
	protected void reduce(
				Text key, 
				Iterable<FloatWritable> values,
				Context context)throws IOException,InterruptedException{

		int num = 0;
		for(IntWritable value: values){
			num++;
		}
		if(num>=10000){
			context.write(key, NullWritable.get());
		}
	}							
}