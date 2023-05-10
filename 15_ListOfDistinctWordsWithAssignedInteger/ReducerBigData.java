package it.polito.bigdata.hadoop.exercise15;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData extends Reducer<
                            Text,
                            NullWritable,
							Text,
                            IntWritable>{

	int number;

	protected void setup(Context context){
		number = 0;
	}

    @Override
	protected void reduce(
                Text key,
                Iterable<NullWritable> values,
                Context context)throws IOException,InterruptedException{

        number = number+1;    
		context.write(new Text(key), new IntWritable(number));
    }
}