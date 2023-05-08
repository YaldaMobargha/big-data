package it.polito.bigdata.hadoop.exercise9;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData extends Mapper<
                            LongWritable,
                            Text,
                            Text,
                            IntWritable>{

	HashMap<String,Integer> wordCount;

	protected void setup(Context context){
		wordCount = new HashMap<String,Integer>();
	}
								
    protected void map(
                LongWritable key,
                Text value,
                Context context)throws IOException,InterruptedException{

		Integer currentWordscount;
        String[] words = value.toString().split("\\s+");

		for(String word : words){
			String cleanedWord = word.toLowerCase();

			if(currentWordscount == null){
				wordCount.put(new String(cleanedWord), new IntWritable(1));
			}else{
				currentWordscount = currentWordscount +1;
				wordCount.put(new String(cleanedWord), new IntWritable(currentWordscount));
			}
		}
    }

	protected void cleanup(Context context)throws IOException,InterruptedException{
		for(Entry<String, Integer> pair : wordCount.entrySet()){
			context.write(new Text(pair.getKey()), new IntWritable(pair.getValue()));
		}
	}

}
