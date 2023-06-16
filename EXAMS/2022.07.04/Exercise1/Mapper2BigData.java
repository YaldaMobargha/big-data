package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class Mapper2BigData extends Mapper<
                            Text,
                            IntWritable,
                            NullWritable,
                            Text>{

    protected void map(
                    Text key, 
                    IntWritable value, 
                    Context context)throws IOException,InterruptedException{

        String sum = value.toString();
        String os = key.toString();
            
		context.write(NullWritable.get(), new Text(os+"_"+sum));

    }
}