package it.polito.bigdata.hadoop.exercise13;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData extends Reducer<
							NullWritable,
							Text,  
							Text,           
							FloatWritable> { 
    	
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	float dailyincome;
    	String date;
		
    	DateValue top1;
    	DateValue top2;
    	
    	top1=null;
    	top2=null;
    	
        // Iterate over the set of values and select the top 2
        for (Text value : values) {

        	String[] parts=value.toString().split("_"); 
    		
        	date=parts[0];
        	dailyincome=Float.parseFloat(parts[1]);

        	if (top1==null || top1.value< dailyincome)
    		{
    			top2=top1;
    			
    			top1=new DateValue();
    			top1.date=new String(date);
    			top1.value=dailyincome;
    		}
    		else
    			{
    				if (top2==null || top2.value<dailyincome)
    				{
    					top2=new DateValue();
    					top2.date=new String(key.toString());
    					top2.value=dailyincome;
    				}
    			}
        }

        // Emit pair (date, value) top1
        // Emit pair (date, value) top2
        context.write(new Text(top1.date), new FloatWritable(top1.value));
        context.write(new Text(top2.date), new FloatWritable(top2.value));
    }
}
