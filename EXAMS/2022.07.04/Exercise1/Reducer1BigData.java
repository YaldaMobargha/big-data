package it.polito.bigdata.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


class Reducer1Bigdata extends Reducer<
                            Text,
                            IntWritable,
                            Text,
                            IntWritable>{
            
    private String maxos;
    private int maxCount;

    @Override
    protected void setup(Context context)throws IOException,InterruptedException{
        this.maxos = "";
        this.maxCount = 0;
    }

    @Override
    protected void reduce(
                    Text key, 
                    Iterable<IntWritable> values, 
                    Context context)throws IOException,InterruptedException{

        int nump = 0;
        String os = key.toString();

        for(IntWritable value:values){
            nump = nump+ value.get();
        }


        if((nump>this.maxCount) || (nump==this.maxCount && os.compareTo(this.maxos)<0 ) || this.maxCount == 0){
            this.maxCount = nump;
            this.maxos = os;
        }   
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text(this.maxos), new IntWritable(maxCount));
    }
}