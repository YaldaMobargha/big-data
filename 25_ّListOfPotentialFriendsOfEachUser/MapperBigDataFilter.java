package it.polito.bigdata.hadoop.exercise25;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData extends Mapper<
                            Text,
                            Text,
                            Text,
                            Text>{

	protected void map(
		        LongWritable key, 
		      	Text value, 
			      Context context)throws IOException, InterruptedException{

		 String[] users = value.toString().split(",");
		
     for (String user:users){
         context.write(new Text(key.toString()),new Text(user));
     }
	}
}