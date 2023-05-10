package it.polito.bigdata.hadoop.exercise14;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData extends Mapper<
                            LongWritable,
                            Text,
                            Text,
                            NullWritable>{

    protected void map(
                LongWritable key,
                Text value,
                Context context)throws IOException,InterruptedException{

        String[] words = value.toString().split("\\s+");

        for(String word: words){
            String cleanedWord = word.toLowerCase();

            context.write(new Text(cleanedWord), NullWritable.get());
        }
    }
}