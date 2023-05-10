package it.polito.bigdata.hadoop.exercise14;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData extends Reducer<
                            Text,
                            NullWritable,
							Text,
                            NullWritable>{

    @Override
	protected void reduce(
                Text key,
                Iterable<NullWritable> values,
                Context context)throws IOException,InterruptedException{

            context.write(new Text(key), NullWritable.get());
    }
}