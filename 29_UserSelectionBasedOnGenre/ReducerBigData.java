package it.polito.bigdata.hadoop.exercise29;

import java.io.IOException;

import javax.lang.model.util.Elements;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData extends Reducer<
                                Text,
                                Text,
                                NullWritable,
                                Text>{

    protected void reduce(
                Text key, 
                Iterable<Text> values, 
                Context context)throws IOException,InterruptedException{
        
        String info=null;
        String Elements;
        
		Elements = 0;

        for(Text value: values){
            
            String reading = value.toString();

			Elements =Elements+1;

            if(reading.startsWith("U")==true){
                info =reading.replace("U", "");
            }
        }

		if(Elements==3){
    		context.write(NullWritable.get(), new Text(info));
		}

    }
}
