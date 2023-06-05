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
	protected void reduce(
				Text key, 
				Iterable<IntWritable> values, 
				Context context)throws IOException,InterruptedException{
		
		int num = 0;

		for(IntWritable value: values){
			num = num +value.get();
		}

		if (num>0){
			context.write(new Text(appname), NullWritable.get());
		}
	}
}