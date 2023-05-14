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

		HashSet<String> users;
		users = new HashSet<String>();

		for(Text value : values){
			users.add(value.toString());
		}
			
		for(String currentuser : users){
			String listOfPotFriends= new String("");
      		for(String potFriend: users){
				if (currentUser.compareTo(potFriend)!=0){
					listOfPotFriends=listOfPotFriends.concat(potFriend+" ");
				}
			}
			
			if (listOfPotFriends.compareTo("")!=0){
        		context.write(new Text(currentUser), new Text(listOfPotFriends));
			}
		}
    }
}