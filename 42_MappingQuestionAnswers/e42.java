package it.polito.bigdata.spark.exercise42;

import org.apache.spark.api.java.*;
import scala.Tuple2;
import org.apache.spark.SparkConf;

public class SparkDriver{
    public static void main(String[] args){

        String inputpathQuestions;
		String inputpathAnswers;
		String outputpath;

		inputpathQuestions = args[0];
		inputpathAnswers = args[1];
		outputpath = args[2];

        SparkConf conf = new SparkConf().setAppName("e42");

        JavaSparkContext obj = new JavaSparkContext(conf);

        JavaRDD<String> infileQues = textFile(inputpathQuestions);
        JavaRDD<String> infileAns = textFile(inputpathAnswers);

        JavaPairRDD <String,String> Quespair = infileQues.mapToPair(line ->{
            String qID;
            String qText;
            String[] parts = line.split(",");
            qID = parts[0];
            qText = parts[2];
            Tuple2<String, String> pair = new Tuple2<String,String>(qID, qText); 
            return pair; 
        });

        JavaPairRDD <String,String> Anspair = infileAns.mapToPair(line ->{
            String qID;
            String aText;
            String[] parts = line.split(",");
            qID = parts[1];
            aText = parts[3];
            Tuple2<String, String> pair = new Tuple2<String,String>(qID, aText); 
            return pair; 
        });

        JavaPairRDD<String, Tuple2<Iterable<String>,Iterable<String>>> QApaired = Quespair.cogroup(Anspair); 

        QApaired.saveAsTextFile(outputpath);
        obj.close();
    }
}
