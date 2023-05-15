package it.polito.bigdata.hadoop.exercise29;

import java.io.IOException;
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
        String userID = parts[0];
        String Genre = parts[1];

		if(Genre.compareTo("Commedia")==0 || Genre.compareTo("Aventure")==0){
			context.write(new Text(userID), new Text("L"));
		}
    }
}