package it.polito.bigdata.hadoop.exercise6;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import javafx.scene.text.Text;

class ReducerBigData extends Reducer<
					Text,
					FloatWritable,
					Text,
					Text>{

	@Override
	protected void reduce(
				Text key,
				Iterable<FloatWritable> values,
				Context context)throws IOException,InterruptedException{
		
		Double min = Double.MAX_VALUE;
		Double max = Double.MIN_VALUE;

		for(FloatWritable value: values){
			if(value.get()> max){
				max=value.get();
			}
			if(value.get()< min){
				min=value.get();
			}
		}
		context.write(new Text(key), new Text("max="+ max +"min=" +min));
	}
}