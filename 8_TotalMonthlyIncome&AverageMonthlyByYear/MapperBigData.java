package it.polito.bigdata.hadoop.exercise8;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData extends Mapper<
                            Text,
                            DoubleWritable,
                            Text,
                            DoubleWritable>{

    protected void map(
                Text key,
                DoubleWritable value,
                Context context)throws IOException,InterruptedException{

        String[] date = key.toString().split("-");

        String yearmonth = new String(date[0]+"-"+date[1]);

        context.write(new Text(yearmonth), new DoubleWritable(Double.parseDouble(value.toString())));
    }
}

