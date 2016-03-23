

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.codec.language.Soundex;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;

public class TwitterPopularTag3 {
	
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.OFF);
		
	    System.setProperty("twitter4j.oauth.consumerKey", "t35z6Nl82swmsPoEw1mWxiUSb");
	    System.setProperty("twitter4j.oauth.consumerSecret", "53Qs77bvYdyRb9xUz67eW2ePmAd0z7muMNgFUQbuD8tRyece5L");
	    System.setProperty("twitter4j.oauth.accessToken", "142020510-jNJqB3Ucdn57C2dD7EyDMxkNcOy8s8cmdRy6os9b");
	    System.setProperty("twitter4j.oauth.accessTokenSecret", "zZIwVHGVHBvnrry2ttu2MvULJY86xZswsJlItPbTxvZQD");
	    
	    SparkConf sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[*]");
	    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
	    JavaStreamingContext ssc = new JavaStreamingContext(jsc, Durations.seconds(10));
	    JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(ssc, new String[]{"android"});
	    
	    JavaDStream<String> english = stream
	    		.map((status)-> status.getText());
	    
	    JavaDStream<String> words = english.flatMap( (status) -> Arrays.asList(SPACE.split(status)));
	    
	    
	   english.print();
	   /*JavaDStream<String> hashTags = words.filter((s) -> s.startsWith("#") && s.length()>3);
	    
	    JavaPairDStream<String, Integer> hashTagsOnes = hashTags.mapToPair((s) -> new Tuple2<String, Integer>(s, 1));
	    
	    JavaPairDStream<String, Integer> topCount60 = hashTagsOnes.reduceByKeyAndWindow((a,b) -> a+b, Durations.minutes(10));
	    
	    JavaPairDStream<String, Integer> topCount60Sorted = topCount60.mapToPair((s) -> s.swap())
	    														.transformToPair((rdd) -> rdd.sortByKey(false))
	    															.mapToPair((s) -> s.swap());
	    
	    */
	    
	    
	    
	    ssc.start();
	    ssc.awaitTermination();


	}

}