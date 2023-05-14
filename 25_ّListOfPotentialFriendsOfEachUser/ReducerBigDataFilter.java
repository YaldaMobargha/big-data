package it.polito.bigdata.hadoop.exercise25;

import java.io.IOException;
import java.util.HashSet;
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

		HashSet<String> potentialFriends;
		potentialFriends = new HashSet<String>();

        String FriendsList;

		for(Text user : values){
			potentialFriends.add(user.toString());
		}
		
        FriendsList = new String("");

		for(String user : potentialFriends){
			listOfPotFriends=listOfPotFriends.concat(user+" ");
		}
        context.write(new Text(key), new Text(FriendsList));
    }
}