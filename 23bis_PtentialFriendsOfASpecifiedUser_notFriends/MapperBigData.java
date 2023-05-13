package it.polito.bigdata.hadoop.exercise23bis;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData extends Mapper<
							LongWritable,
							Text,
							NullWritable,
                            Text>{

	String SpecifiedUser;

	protected void setup(Context context){
		SpecifiedUser = context.getConfiguration().get("username");
    }
	
	protected void map(
				LongWritable key, 
				Text value, 
				Context context)throws IOException, InterruptedException{

		String[] parts = value.toString().split(",");

		if(parts[0].compareTo(SpecifiedUser)==0){
			context.write(NullWritable.get(),new Text(parts[1]));
		}
        if(parts[1].compareTo(SpecifiedUser)==0){
			context.write(NullWritable.get(),new Text(parts[0]));
		}
	}
}