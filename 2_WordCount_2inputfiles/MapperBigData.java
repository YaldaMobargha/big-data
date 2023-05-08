package it.polito.bigdata.hadoop.exercise2;

import java.io.IOException;

import javax.swing.text.AbstractDocument.Content;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData extends Mapper<
                    LongWritable,
                    Text,
                    Text,
                    IntWritable>{

    protected void map(
        LongWritable key,
        Text value,
        Context context) throws IOException, InterruptedException{

            String[] words = value.toString().split("\\s+");
 
            for(String word : words){
                String cleanedWord = word.toLowerCase();
                
                context.write(new Text(cleanedWord), new IntWritable(1));
            }
        }
}