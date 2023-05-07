package it.polito.bigdata.hadoop.exercise5;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData extends Reducer<
                    Text,
                    FloatWritable,
                    Text,
                    FloatWritable>{

    @Override
    protected void Reducer(Context context)throws IOException,InterruptedException{
        int num = 0;
        int sum = 0;
        
        for(FloatWritable value : values){
            sum = sum+value.get();
            num = num +1;
        }

        context.write(new Text(key), new FloatWritable((float)sum/num));
    }

}