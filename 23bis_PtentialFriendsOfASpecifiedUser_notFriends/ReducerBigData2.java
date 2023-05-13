package it.polito.bigdata.hadoop.exercise23bis;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData extends Reducer<
                            NullWritable,
                            Text,
                            Text,
                            NullWritable>{

    @Override

    protected void reduce(
                NullWritable key,
                Iterable<Text> values,
                Conetxt context)throws IOException,InterruptedException{

		String finalList = new String("");
		ArrayList<String> potFriends = new ArrayList<String>();
		
		for (Text value : values) {
			if (potFriends.contains(value.toString()) == false)
				potFriends.add(value.toString());
		}

		for (String potFriend : potFriends) {
			finalList = finalList.concat(potFriend + " ");
		}

		context.write(new Text(finalList), NullWritable.get());
        
    }
}
