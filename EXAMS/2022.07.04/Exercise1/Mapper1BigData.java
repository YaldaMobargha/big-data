package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class Mapper1BigData extends Mapper<
                            LongWritable,
                            Text,
                            Text,
                            IntWritable>{

    protected void map(
                    LongWritable key, 
                    Text value, 
                    Context context)throws IOException,InterruptedException{

        String[] parts = value.toString().split(",");
        String RDate = parts[1];
        String os = parts[2];

        if(RDate.compareTo("2021/07/04")>=0 && RDate.compareTo("2022/07/03")<= 0 ){

            context.write(new Text(os), new IntWritable(1));

        }
    }
}