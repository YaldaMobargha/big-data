package it.polito.bigdata.hadoop.exercise26;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
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

	private HashMap<String,Integer> dictionary;

	protected void setup(Conetxt context){
		
		String line;
		String word;
		Integer IntValue;

		dictionary = new HashMap<String,Integer>();
		URI[] Cache = context.getCache();
		BufferedReader stopwords = new BufferedReader(new FileReader(new File(Cache[0].getPath())));

		while ((line = fileStopWords.readLine()) != null) {

			String[] record = line.split("\t");
			intValue = Integer.parseInt(record[0]);
			word = record[1];

			dictionary.put(word, intValue);
		}
	}

	protected void map(
				LongWritable key, // Input key type
				javax.xml.soap.Text value, // Input value type
				Context context) throws IOException, InterruptedException {
		
		String converted;
		Integer integerValue;

		String[] parts = value.toString().split("\\s+");

		converted = new String("");

		for(String word : words){
						
			integerValue = dictionary.get(word.toUpperCase());
			converted = converted.concat(integerValue + " ");
		}

		context.write(NullWritable.get(), new Text(converted));
	}


}