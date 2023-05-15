package it.polito.bigdata.hadoop.exercise28;

import java.io.IOException;

import javax.xml.soap.Text;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

class MapperType1BigData extends Mapper<
                                LongWritable,
                                Text,
                                Text,
                                Text>{

    protected void map(
                LongWritable key, 
                Text value, 
                Context context)throws IOException,InterruptedException{
        
        String[] parts = value.toString().split(",");
        String qID = parts[0];
        String qText = parts[2];

        context.write(new Text(qID), new Text("Q:"+qID+","+qText));
    }
}