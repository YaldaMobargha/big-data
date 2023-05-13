package it.polito.bigdata.hadoop.exercise23bis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
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
	ArrayList<String> Friends;

	protected void setup(Context context)throws IOException, InterruptedException{
		SpecifiedUser = context.getConfiguration().get("username");

		Friends = new ArrayList<String>();

		URI[] CachedFiles = context.getCacheFiles();
		BufferedReader fileFriends = new BufferedReader(new FileReader(new File(new Path(CachedFiles[0].getPath()).getName())));
		
		while ((line = fileFriends.readLine()) != null) {
			friends.add(line);
		}

		fileFriends.close();
    }
	
	protected void map(
				LongWritable key, 
				Text value, 
				Context context)throws IOException, InterruptedException{

		String[] parts = value.toString().split(",");

		if(parts[0].compareTo(SpecifiedUser)!=0 && Friends.contain(parts[1])==true){
			if(Friends.contain(parts[0])==false){
				context.write(NullWritable.get(),new Text(parts[0]));
			}
		}
        if(parts[1].compareTo(SpecifiedUser)==0 && Friends.contain(parts[0])==true){
			if(Friends.contain(parts[1])==false){
				context.write(NullWritable.get(),new Text(parts[1]));
			}
		}
	}
}
