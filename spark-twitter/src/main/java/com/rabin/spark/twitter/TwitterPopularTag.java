package com.rabin.spark.twitter;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;

public class TwitterPopularTag {
	
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.OFF);
		
	    System.setProperty("twitter4j.oauth.consumerKey", "lPoNSWiMydRgKfMlNRifduWbD");
	    System.setProperty("twitter4j.oauth.consumerSecret", "Js5i3jro1UXWRymDtqGpiPwmiSiQjMBuRtYozAbG0elLPXvoCU");
	    System.setProperty("twitter4j.oauth.accessToken", "142020510-jNJqB3Ucdn57C2dD7EyDMxkNcOy8s8cmdRy6os9b");
	    System.setProperty("twitter4j.oauth.accessTokenSecret", "zZIwVHGVHBvnrry2ttu2MvULJY86xZswsJlItPbTxvZQD");

	    SparkConf sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[*]");
	    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
	    JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(ssc);
	    
	    JavaDStream<String> english = stream.filter((f)->f.getUser().getLang().equals("en"))
	    		.map((status)-> status.getText().replaceAll("[^\\x00-\\x7F]", ""));
	    
	    JavaDStream<String> words = english.flatMap( (status) -> Arrays.asList(SPACE.split(status)));
	    
	    
	   //english.print();
	   JavaDStream<String> hashTags = words.filter((s) -> s.startsWith("#") && s.length()>3);
	    
	    JavaPairDStream<String, Integer> hashTagsOnes = hashTags.mapToPair((s) -> new Tuple2<String, Integer>(s, 1));
	    
	    JavaPairDStream<String, Integer> topCount60 = hashTagsOnes.reduceByKeyAndWindow((a,b) -> a+b, Durations.minutes(10));
	    topCount60.mapToPair((s) -> new Tuple2<Integer,String>(s._2,s._1)).transformToPair((rdd) -> rdd.sortByKey(false)).cache().print();
	    //topCount60.print();
	    /*JavaPairDStream<String, Integer> topCount60Sorted = topCount60.transformToPair((rdd) -> rdd.sortByKey(true));
	    
	    topCount60Sorted.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>(){
			@Override
			public Void call(JavaPairRDD<String, Integer> rdd) throws Exception {
				List<Tuple2<String, Integer>> topList = rdd.take(10);
				System.out.println(String.format("Popular topics in last 60 seconds (%s total):", rdd.count()));
				for(Tuple2<String, Integer> list : topList){
					System.out.println(String.format("%s (%s tweets)", list._1, list._2));
				}
				return null;
			}
	    	
	    });*/
	    
	    ssc.start();
	    ssc.awaitTermination();


	}

}