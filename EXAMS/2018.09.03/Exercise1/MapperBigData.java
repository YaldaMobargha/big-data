package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData extends Mapper<
							LongWritable,
							Text,
							NullWritable,
							Text>{
	
	double highest= Double.MIN_VALUE;
	String firstTime = null;

	protected void main(
				LongWritable key, 
				Text value, 
				Context context)throws IOException,InterruptedException{

		String[] parts = value.toString().split(",");
		String sID = parts[0];
		String date = parts[1];
		String time = parts[2];
		String year = date.split("/")[0];
		Double price = double.parseDouble(parts[3]);
		
		if(year.compareTo("2017")==0 && sID.compareTo("GOOG")==0){
			if(price>=highest){
				highest = price;

				String timeStamp = new String(date+","+time);

				if(timeStamp.compareTo(firstTime)<=0 || firstTime==null){
					firstTime=timeStamp;
				}
			}
		}
	}

	protected void cleanup(Context context)throws IOException,InterruptedException{
		if(firstTime!= null){
			context.write(NullWritable.get() ,new Text(highest+","+firstTime));
		}
	}
}






		