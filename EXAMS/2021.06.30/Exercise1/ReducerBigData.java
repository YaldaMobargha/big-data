package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.function.IntUnaryOperator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData extends Reducer<
							Text,
							Text,
							Text,
							NullWritable>{

	@Override
	protected void reduce(
					Text key, 
					Iterable<Text> values, 
					Context context)throws IOException, InterruptedException{
				
		int numInside = 0;
		int numOutside = 0;

		for(Text value:values){
			if(value.get().equals("T")){
				numInside++;
			}else{
				numOutside++;
			}
		}

		if(numInside>=10000 && numOutside>=10000){
			context.write(key, NullWritable.get());
		}
	}
}