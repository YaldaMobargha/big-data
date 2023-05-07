package it.polito.bigdata.hadoop.exercise4;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData extends Reducer<
					Text,
					Text,
					Text,
					Text> {

	@Override
	protected void reduce(
				Text key,
				Iterable<Text> values,
				Context context)throws IOException,InterruptedException{

			String datesList = new String();
				
			for(String value : values){
				if(datesList.length()==0){
					datesList = new String(date.toString());
				}else{
					datesList = datesList.concat("," + date.toString());
				}
			}

			context.write(new Text(key), new Text(datesList));

	}
}