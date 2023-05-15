package it.polito.bigdata.hadoop.exercise28;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import javafx.scene.text.Text;

class ReducerBigData extends Reducer<
                                Text,
                                Text,
                                NullWritable,
                                Text>{

    protected void reduce(
                Text key, 
                Iterable<Text> values, 
                Context context)throws IOException,InterruptedException{
        
        String question=null;
        ArrayList<String> answers = new ArrayList<String>();
        
        for(Text value: values){
            
            String reading = value.toString();

            if(reading.startsWith("Q:")==true){
                question =reading.replaceFirst("Q:", "");

            }else{
                answers.add(reading.replaceFirst("A:", ""));
            }
        }

        for(String answer:answers){
            context.write(NullWritable.get(), new Text(question+","+answer));
        }
    }
}
