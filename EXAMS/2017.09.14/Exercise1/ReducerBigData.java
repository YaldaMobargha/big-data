package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData extends Reducer<
							Text,
							Text,
							Text,
							FloatWritable>{

	@Override
	protected void reduce(
				Text key, 
				Iterable<Text> values, 
				Context context)throws IOException, InterruptedException{

		Float cancel= 0;
		Float total = 0;
		Float percentage;
	
		for(String value: values){
			total = total+1;
			if(value.get().compareTo("yes")==0){
				cancel = cancel+1;
			}
		}

		percentage = (total/cancel)*100;

		if(percentage>1){
			context.write(key, new FloatWritable(percentage));
		}
	}
}