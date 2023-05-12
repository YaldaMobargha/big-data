package it.polito.bigdata.hadoop.exercise21;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

class MapperBigData extends Mapper<
							LongWritable,
							Text,
							NullWritable,
							Text>{


	private ArrayList<String> stopwords;

	protected void setup(Context context){
		String nextLine;

		stopwords = new ArrayList<String>();
		URI[] urisCachedFiles = context.getCacheFiles();
		BufferedReader fileStopwords = new BufferedReader(new FileReader(new File(urisCachedFiles[0].getPath())));

		while ((nextLine = fileStopwords.readLine()) != null){
			stopwords.add(nextLine);
		}

		fileStopwords.close();
	}
	
	protected void map(
				LongWritable key, 
				Text value, 
				Context context)throws IOException, InterruptedException{

		String cleanedsentence = new String("");
		boolean contain;

		String[] words = value.toString().split("\\s+");

		for(String word : words){
			if(stopwords.contains(word)==true){
				contain == true,
			}else{
				contain == false;
			}

			if(stopwords == false){
				cleanedsentence.concat(word+" ");
			}
		}

		context.write(NullWritable.get(), new Text(cleanedsentence));
	}
}