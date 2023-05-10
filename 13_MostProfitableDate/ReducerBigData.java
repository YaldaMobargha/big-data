package it.polito.bigdata.hadoop.exercise13;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData extends Reducer<
							NullWritable,
							DateIncome,
							Text,
							FloatWritable>{

	@Override

	protected void reduce(
				NullWritable key,
				Iterable<DateIncome> values,
				Context context)throws IOException,InterruptedException{

		float dailyincome;
		String date;

		DateIncome globaltop1 = new DateIncome();
		globalTop1.setIncome(Float.MIN_VALUE);
		globalTop1.setDate(null);

		for (DateIncome value : values) {

			date = value.getDate();
			dailyIncome = value.getIncome();

			if ( dailyIncome > globalTop1.getIncome() ||
			    (dailyIncome == globalTop1.getIncome() && date.compareTo(globalTop1.getDate()) < 0)) {
			
				globalTop1 = new DateIncome();
				globalTop1.setDate(date);
				globalTop1.setIncome(dailyIncome);
			}
		}
		
		context.write(new Text(globalTop1.getDate()), new FloatWritable(globalTop1.getIncome()));
	}
}
