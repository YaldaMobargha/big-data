package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

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
					Iterable<Text> value, 
					Context context)throws IOException,InterruptedException{
		
		int g=0;
		int l=0;

		for(String value: values){
			if(Greater.compareTo(value.get())==0){
				g= g+1;
			}else{
				l=l+1;
			}
		}
		if(g>=1 && l>=1){
			context.write(key, NullWritable.get());
	}
}