package it.polito.bigdata.hadoop.exercise3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData extends Reducer<
					Text,
					IntWritable,
					Text,
					IntWritable>{

	@Override
	
	protected void reduce(
				Text,
				IntWritable,
				Context context)throws IOException,InterruptedException{

					int num = 0
					
					for(IntWritable value : values){
						num = num +value.get();
					}

				context.write(new Text(key), new IntWritable(num));
				}
}