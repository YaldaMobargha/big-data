package it.polito.bigdata.hadoop.exercise13;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData extends Mapper<
							Text,
							Text,
							NullWritable,
							Text>{

	DateValue top1;
	DateValue top2;

	protected void setup(Context context){
		DateIncome top1;
		top1 = null;
		top1 = null;
	}

	protected void map(
				Text key,
				Text value,
				Context context){
			
		float dailyincome = Float.parseFloat(value.toString());
		String date = new String(key.toString());

		if(top1 = null || dailyincome>top1.value || (top1.value==dailyincome && date.compareTo(top1.date)<0)){
			top2 = top1;
			top1 = new DateValue();
			top1.date = date;
			top1.value = value;
		}else{
			if(top2 = null || dailyincome>top2.value || (top2.value==dailyincome && date.compareTo(top2.date)<0)){
				top2 = new DateValue();
				top2.date = date;
				top2.value = value;
			}
		}
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(NullWritable.get(), new Text(top1.date + "_" + top1.value));
			context.write(NullWritable.get(), new Text(top2.date + "_" + top2.value));
		}
	}


}
