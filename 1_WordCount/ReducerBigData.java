package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.omg.CORBA.Context;

class ReducerBigData extends Reducer<
                Text,
                IntWritable,
                Text,
                IntWritable>{

    @Override

    protected void reduce(
        Text key,
        Iterable<IntWritable> value,
        Context context)throws IOException, InterruptedException{

            int num = 0;

            for(IntWritable value : values){
                num = num+ value.get();
            }

            context.write(key, new IntWritable(num));
        }
}