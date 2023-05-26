package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
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

		int ram = 0;
		int drive = 0;

		for(Text value: values){
			if(value.get().compareTo("RAM")==0){
				ram++;
			}
			if(value.get().compareTo("hard drive")==0){
				drive++;
			}
		}
		if( ram>=1 && drive>=1){
			context.write(key, NullWritable.get());
		}
	}
}