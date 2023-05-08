package it.polito.bigdata.hadoop.exercise8;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData extends Reducer<
                            Text,
                            DoubleWritable,
                            Text,
                            DoubleWritable>{

    @Override
    protected void reduce(
					Text key,
					DoubleWritable value,
					Context context)throws IOException,InterruptedException{

        int monthlySum = 0;

        for(DoubleWritable value : values){
            monthlySum = monthlySum + value.get();
        }

        context.write(new Text(key), new DoubleWritable(monthlySum));
    }
}