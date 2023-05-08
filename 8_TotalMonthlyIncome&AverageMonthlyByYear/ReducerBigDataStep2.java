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
					Iterable<DoubleWritable> values,
					Context context)throws IOException,InterruptedException{

        int yearlySum = 0;
        int num = 0;

        for(DoubleWritable value : values){
            yearlySum = yearlySum + value.get();
            num = num +1;
        }

        context.write(new Text(key), new DoubleWritable(yearlySum/num));
    }
}