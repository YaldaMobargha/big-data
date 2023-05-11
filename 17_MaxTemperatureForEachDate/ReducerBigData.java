package it.polito.bigdata.hadoop.exercise17;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData extends Reducer<
                            Text,
                            FloatWritable,
                            Text,
                            FloatWritable>{

    @Override
    protected void reduce(
                Text key,
                Iterable<FloatWritable> values,
                Conetxt context)throws IOException,InterruptedException{

       float Max = Float.MIN_VALUE;
       
       for(FloatWritable value : values){
            if(value.get()>Max){
                Max = value.get();
            }    
        }

        context.write(new Text(key), new FloatWritable(Max));
    }
}