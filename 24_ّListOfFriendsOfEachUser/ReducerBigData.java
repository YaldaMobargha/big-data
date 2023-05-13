package it.polito.bigdata.hadoop.exercise24;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData extends Reducer<
                            Text,
                            Text,
                            Text,
                            Text>{

    @Override
    protected void reduce(
                Text key,
                Iterable<Text> values,
                Conetxt context)throws IOException,InterruptedException{

       String listOfFriends= new String("");

       for(String value : values){
        listOfFriends=listOfFriends.concat(value.toString()+" ");
		}

        context.write(new Text(key+":"), new Text(listOfFriends));
        
    }
}