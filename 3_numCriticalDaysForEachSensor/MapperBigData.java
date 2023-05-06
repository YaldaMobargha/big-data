package it.polito.bigdata.hadoop.exercise3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.omg.CORBA.Context;

class MapperBigData extends Mapper<
                    Text,
                    Text,
                    Text,
                    IntWritable>{

    private static Double threshold = new Double(50);

    protected void map(
        Text key,
        Text values,
        Context context) throws IOException, InterruptedException{

        String[] parts = key.toString().split(",");
        String sID = parts[0];
        Double sensorValue = new Double(value.toString());

        for(sensorValue.compareTo(threshold)>0){
            context.write(new Text(sID), new IntWritable(1));
        }

    }
}
