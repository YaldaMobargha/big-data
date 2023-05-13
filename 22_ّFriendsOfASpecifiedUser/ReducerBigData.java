package it.polito.bigdata.hadoop.exercise22;

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

       String result = new String("");
       
       for(String value : values){
            result = result.concat(value.toString()+" ");
        }

        context.write(new Text(result), NullWritable.get());
    }
}