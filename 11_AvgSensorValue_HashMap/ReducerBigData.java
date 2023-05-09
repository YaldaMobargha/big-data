package it.polito.bigdata.hadoop.exercise11;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData extends Reducer<
                            Text,           
                            SumCount,  
                            Text,         
                            SumCount> {  
                
    @Override
    protected void reduce(
                    Text key, 
                    Iterable<SumCount> values, 
                    Context context) throws IOException, InterruptedException{

    	int count=0;
    	float sum=0;
        
        for (SumCount value : values) {
        	sum=sum+value.getSum();
        	count=count+value.getCount();
        }

    	SumCount sumCountPerSensor= new SumCount();
    	sumCountPerSensor.setCount(count);
    	sumCountPerSensor.setSum(sum);
    	

        // Emits pair (sensor_id, sum-count = average)
        context.write(new Text(key), sumCountPerSensor);
    }
}
