package it.polito.bigdata.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class Reducer1Bigdata extends Reducer<
                            NullWritable,
                            Text,
                            Text,
                            NullWritable>{
            

    @Override
    protected void reduce(
                    NullWritable key, 
                    Iterable<Text> values, 
                    Context context)throws IOException,InterruptedException{

        String maxos = "";
        int maxCount = 0;


        for(Text value:values){
            
            String[] parts = value.toString().split("_");
            int nump = Integer.parseInt(parts[1]);
            String os = parts[0];

            if((nump>maxCount) || (nump==maxCount && os.compareTo(maxos)<0 ) || maxCount == 0){
                maxCount = nump;
                maxos = os;
            } 
        }

        context.write(new Text(maxos), NullWritable.get()); 
    }
}