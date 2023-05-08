package it.polito.bigdata.hadoop.exercise7;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import javafx.scene.text.Text;

class ReducerBigData extends Reducer<
							Text,
							Text,
							Text,
							Text>{

	@Override
	protected void reduce(
					Text key,
					Iterable<Text> values,
					Context context)throws IOException,InterruptedException{
		
		String sentencesList = new String();

		for(String value : values){
			sentencesList = sentencesList.concat(value + ",");
		}
		context.write(new Text(key), new Text(sentencesList));
	}
}