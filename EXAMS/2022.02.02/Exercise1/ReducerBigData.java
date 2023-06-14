package it.polito.bigdata.hadoop;

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
				Text key, 
				Iterable<IntWritable> values, 
				Context context)throws IOException,InterruptedException{

		int num = 0;
		int diff = 0;
		
		for (IntWritable value:values){
			num++;
			diff = diff+ value.get();
		}

		if(diff>0){
			context.write(key, new IntWritable(num));
		}
	}
}