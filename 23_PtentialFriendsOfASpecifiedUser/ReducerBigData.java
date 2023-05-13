package it.polito.bigdata.hadoop.exercise23v2;

import java.io.IOException;
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

       for(String value : values){
			context.write(new Text(value.toString()), NullWritable.get());
        }
    }
}