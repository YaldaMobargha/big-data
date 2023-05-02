package it.polito.bigdata.spark.exercise44;

import org.apache.spark.api.java.*;
import scala.Tuple2;
import java.util.ArrayList;
import org.apache.spark.SparkConf;

public class SparkDriver{
    public static void main(String[] args){
        String inputpathWatched;
        String inputpathPreferences;
        String inputpathMovies;
        String outputpath;
        
        Double threshold;

        inputpathWatched = args[0];
        inputpathPreferences = args[1];
        inputpathMovies = args[2];
        outputpath = args[3];
        
        threshold = Double.parseInt(args[4]);

        SparkConf conf = new SparkConf().setAppName("e44");
        JavaSparkContext obj = new JavaSparkContext(conf);

        JavaRDD<String> infileWatched = obj.textFile(inputpathWatched);
        JavaRDD<String> infilePrefer = obj.textFile(inputpathPreferences);
        JavaRDD<String> infileMovies = obj.textFile(inputpathMovies);

        JavaPairRDD <String,String> UserMoviepair = infileWatched.mapToPair(line ->{
            String UserID;
            String mID;
            String[] parts = line.split(",");
            UserID = parts[0];
            mID = parts[1];
            Tuple2<String, String> pair = new Tuple2<String,String>(mID,UserID); 
            return pair; 
        });

        JavaPairRDD <String,String> UserGenrepair = infilePrefer.mapToPair(line ->{
            String UserID;
            String genre;
            String[] parts = line.split(",");
            UserID = parts[0];
            genre = parts[1];
            Tuple2<String, String> pair = new Tuple2<String,String>(UserID,genre); 
            return pair; 
        });

        JavaPairRDD <String,String> MovieGenrepair = infileMovies.mapToPair(line ->{
            String mID;
            String genre;
            String[] parts = line.split(",");
            genre = parts[2];
            mID = parts[0];
            Tuple2<String, String> pair = new Tuple2<String,String>(mID,genre); 
            return pair; 
        });

        JavaPairRDD <String,Tuple2<String, String>> JoinedMovieGenre = UserMoviepair.join(MovieGenrepair);

        JavaPairRDD <String,String> JoinedUG = JoinedMovieGenre.mapToPair(Tuple2<String,Tuple2<String, String>> select ->{
            Tuple2<String,String> movieGenre = new Tuple2<String,String>(select._2()._1(),select._2()._2());
            return movieGenre;
        });

        JavaPairRDD<String,Integer> valuetoOne = JoinedUG.mapValues(mgenre ->1);

        JavaPairRDD<String,Integer> numWatched = valuetoOne.reduceByKey((value1,value2) -> value1+value2);

        JavaPairRDD<String,String> diff = JoinedUG.subtract(UserGenrepair);

        JavaPairRDD<String,Integer> valuetoOnePref = diff.mapValues(mgenre ->1);

        JavaPairRDD<String,Integer> numNotPref = valuetoOnePref.reduceByKey((value1,value2) -> value1+value2);

        JavaRDD<String> misleading = numNotPref.join(numWatched).filter(pair -> {
					int notLiked = pair._2()._1();
					int numWatchedMovies = pair._2()._2();

					if ((double) notLiked > threshold * (double) numWatchedMovies) {
						return true;
					} else
						return false;
				}).keys();
            
        misleading.saveAsTextFile(outputpath);
        obj.close();
    }
}
