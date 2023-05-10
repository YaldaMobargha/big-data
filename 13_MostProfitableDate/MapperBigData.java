package it.polito.bigdata.hadoop.exercise13;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.omg.CORBA.Context;
import javafx.scene.text.Text;

class MapperBigData extends Mapper<
							Text,
							Text,
							NullWritable,
							DateIncome>{

	private DateIncome top1;
	
	protected void setup(Context context){
		top1 = new DateIncome();
		top1.setIncome(Float.MIN_VALUE);
		top1.setDate(dateValue:null);
	}

	protected void map(
				Text key,
				Text value,
				Context context)throws IOException,InterruptedException{

		float dailyincome = Float.parseFloat(value.toString());
		String date = new String(key.toString());

		if(dailyincome>top1.getIncome() || (top1.getIncome()==dailyincome && date.compareTo(top1.getDate())<0)){
			top1 = new DateIncome();
			top1.setIncome(dailyincome);
			top1.setDate(date);
		}
	}

	protected void cleanup(Context context)throws IOException,InterruptedException {
		context.write(NullWritable.get(),top1);
	}
}
