package it.polito.bigdata.spark.exercise45;

import org.apache.spark.api.java.*;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.HashMap;
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

        SparkConf conf = new SparkConf().setAppName("e45");
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

        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> userWatchedLikedGenres = UserGenrepair.cogroup(JoinedUG);

        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> misList= userWatchedLikedGenres.filter((Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> LikedandWatchedPair) -> {
            ArrayList<String> likedgenres = new ArrayList<String>();

            for (String liked : LikedandWatchedPair._2()._1()){
                likedgenres.add(liked);
            }

            int numWatchedMovies = 0;
		    int notLiked = 0;

            for(String watchedGenre : LikedandWatchedPair._2()._2()){
                numWatchedMovies++;
                if(likedgenres.contain(watchedGenre) == false){
                    notLiked++;
                }
            }

            if ((double) notLiked > threshold * (double) numWatchedMovies) {
                return true;
            } else
                return false;

        });

        JavaPairRDD<String,String> userMisleadGenre =misList.flatMapValues((Tuple2<Iterable<String>, Iterable<String>> LWLG) -> {
            ArrayList<String> selectedGenres = new ArrayList<String>();

			ArrayList<String> likedGenres = new ArrayList<String>();

			for (String likedGenre : listWatchedLikedGenres._1()) {
				likedGenres.add(likedGenre);
            }

            HashMap<String, Integer> numGenres = new HashMap<String, Integer>();

            for (String watchedGenre : listWatchedLikedGenres._2()) {
                if (likedGenres.contains(watchedGenre) == false) {
                    Integer num = numGenres.get(watchedGenre);

                    if (num == null) {
                        numGenres.put(watchedGenre, new Integer(1));
                    } else {
                        numGenres.put(watchedGenre, num + 1);
                    }
                }
            }

            for(String genre : numGenres.keyset()){
                if(numGenres.get(genre) >= 5){
                    selectedGenres.add(genre);
                }
            }

            return selectedGenres;

        });

        userMisleadGenre.saveAsTextFile(outputpath);
        obj.close();
    }
}
