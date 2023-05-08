package it.polito.bigdata.hadoop.exercise7;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import javafx.scene.text.Text;

class MapperBigData extends Mapper<
					Text,
					Text,
					Text,
					Text>{

	protected void map(
					Text key,
					Text value,
					Context context)throws IOException,InterruptedException{

		String[] parts = value.toString().split("\\s+");

		for(String part : parts){
			String cleanedWord = part.toLowerCase();

			if(cleanedWord.compareTo("and")!= 0 && cleanedWord.compareTo("or")!= 0 && cleanedWord.compareTo("not")!= 0){
				context.write(new Text(cleanedWord), new Text(cleanedWord));
			}
		}
	}
}