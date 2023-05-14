package it.polito.bigdata.hadoop.exercise27;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.omg.CORBA.Context;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

class MapperBigData extends Mapper<
							LongWritable,
							Text,
							NullWritable,
							Text>{
	private ArrayList<String> Rules;

	protected void setup(Context context) throws IOException, InterruptedException {
		String nextLine;

		Rules = new ArrayList<String>();
		URI[] CachedFiles = context.getCacheFiles();
		BufferedReader rulesFile = new BufferedReader(new FileReader(new File(CachedFiles[0].getPath())));

		while ((nextLine = rulesFile.readLine()) != null) {
			rules.add(nextLine);
		}

		rulesFile.close();
	}

	private String BusinessCategory(String gender, String year){
		String CATEGORY = new String("unknown");

		for(String rule : Rules){
			String[] parts = rule.split(" ");
			if(parts[0].compareTo("Gender" + gender)==0 && parts[2].compareTo("YearOfBirth" + year) == 0 ){
				CATEGORY = parts[4];
			}
		}

		return CATEGORY;
	}

	private void map(
				LongWritable key,
				Text value,
				Context context)throws IOException,InterruptedException{

		String category;

		String[] parts = value.toString().split(",");
		String Gender = parts[3];
		String Year = parts[4];
		category = BusinessCategory(Gender, Year);

		context.write(NullWritable.get(), new Text(value.toString() +","+ category));
	}
}