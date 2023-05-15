package it.polito.bigdata.hadoop.exercise28;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

class MapperType2BigData extends Mapper<
                                LongWritable,
                                Text,
                                Text,
                                Text>{

    protected void map(
                LongWritable key, 
                Text value, 
                Context context)throws IOException,InterruptedException{
        
        String[] parts = value.toString().split(",");
        String aID = parts[0];
		String qID = parts[1];
        String qText = parts[3];

        context.write(new Text(qID), new Text("A:"+aID+","+qText));
    }
}