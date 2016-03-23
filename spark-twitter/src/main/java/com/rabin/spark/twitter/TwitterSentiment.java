package com.rabin.spark.twitter;

import java.util.Arrays;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import scala.reflect.internal.Trees.Return;
import twitter4j.Status;

public class TwitterSentiment {
	
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.OFF);
		
	    System.setProperty("twitter4j.oauth.consumerKey", "t35z6Nl82swmsPoEw1mWxiUSb");
	    System.setProperty("twitter4j.oauth.consumerSecret", "53Qs77bvYdyRb9xUz67eW2ePmAd0z7muMNgFUQbuD8tRyece5L");
	    System.setProperty("twitter4j.oauth.accessToken", "142020510-jNJqB3Ucdn57C2dD7EyDMxkNcOy8s8cmdRy6os9b");
	    System.setProperty("twitter4j.oauth.accessTokenSecret", "zZIwVHGVHBvnrry2ttu2MvULJY86xZswsJlItPbTxvZQD");
	    
	    SparkConf sparkConf = new SparkConf().setAppName("TwitterSentiment").setMaster("local[*]");
	    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
	    
	    
	    
	    JavaStreamingContext ssc = new JavaStreamingContext(jsc, Durations.seconds(10));
	    
	    Broadcast<Set<String>> stopWords = jsc.broadcast(WordReader.readWords("stop-words.txt"));
	    Broadcast<Set<String>> posWords = jsc.broadcast(WordReader.readWords("pos-words.txt"));
	    Broadcast<Set<String>> negWords = jsc.broadcast(WordReader.readWords("neg-words.txt"));
	    
	    JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(ssc , new String[] {"bsnl"});
	    
	    JavaDStream<String> english = stream
	    		.map((status)-> status.getText().replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase());
	    
	    
	    
	    JavaDStream<Tuple2<String, String>> swFiltered = english.map((f) -> {
	    	Set<String> sws = stopWords.value();
	    	String temp= f;
	    	for(String sw:sws )
	    		f = f.replaceAll("\\b" + sw + "\\b", "");
	    	
	    	return new Tuple2<String, String>(f.trim(), temp);
	    	
	    } );
	    
	    JavaPairDStream<String, Float> posSentiment = swFiltered.mapToPair((inp) -> {
	    	String[] words = SPACE.split(inp._1);
	    	int numWords = words.length;
	    	int numPosWords = 0;
	    	Set<String> pw = posWords.value();
	    	
	    	for (String word : words)
	    	{
	    	    if (pw.contains(word))
	    	        numPosWords++;
	    	}
	    	//System.out.println("ppp "+ numPosWords + numWords);
	    	return new Tuple2<String, Float>(inp._2,
	    	     ((float)(numPosWords+1)  / (numWords+pw.size()))) ;
	    	
	    	
	    });
	    JavaPairDStream<String, Float> negSentiment = swFiltered.mapToPair((inp) -> {
	    	String[] words = SPACE.split(inp._1);
	    	int numWords = words.length;
	    	int numNegWords = 0;
	    	Set<String> nw = negWords.value();
	    	
	    	for (String word : words)
	    	{
	    	    if (nw.contains(word)){
	    	    	numNegWords++;
	    	    }
	    	}
	    	//System.out.println("nnn "+ numNegWords + numWords);
	    	return new Tuple2<String, Float>(inp._2,
	    	     ((float)(numNegWords+1) / (numWords +nw.size()) )) ;
	    	
	    	
	    });
	   	posSentiment.join(negSentiment).mapToPair((f) -> new Tuple2<>(f._1, f._2._1 - f._2._2 > 0 ? "POSITIVE": "NEGATIVE")).print(50);
	    
	    
	    //JavaDStream<String> words = english.flatMap( (status) -> Arrays.asList(SPACE.split(status)));
	    
	   // words.transform((rdd) -> rdd.subtract(stopwords)).print(10);;
	   
	   //english.print();
	   //JavaDStream<String> hashTags = words.filter((s) -> s.startsWith("#") && s.length()>3);
	   
	    
	    //avaPairDStream<String, Integer> hashTagsOnes = hashTags.mapToPair((s) -> new Tuple2<String, Integer>(s, 1));
	   
	    
	    
	    /*JavaPairDStream<String, Integer> topCount60 = hashTagsOnes.reduceByKeyAndWindow((a,b) -> a+b, Durations.minutes(10));
	    
	    JavaPairDStream<String, Integer> topCount60Sorted = topCount60.mapToPair((s) -> s.swap())
	    														.transformToPair((rdd) -> rdd.sortByKey(false))
	    															.mapToPair((s) -> s.swap());
	    
	    
	    
	    topCount60Sorted.foreachRDD( (rdd) -> {
	    	List<Tuple2<String, Integer>> topList = rdd.take(10);
	    	Map<String, Tuple2<String, Integer>> resultmap = new HashMap<>(10);
			System.out.println("----------------------------------------------------");
			System.out.println(String.format("Popular topics out of %s total topics received:\n", rdd.count()));
			Soundex sx = new Soundex();
			for(int i =0 ;i<topList.size() ;i++)
			{
				String key = sx.encode(topList.get(i)._1);
				if(!resultmap.containsKey(key))
				{
					resultmap.put(key, topList.get(i));
				}else{
					Tuple2<String, Integer> old = resultmap.get(key);
					Tuple2<String, Integer> newval = new Tuple2<>(topList.get(i)._1 +"/"+ old._1, topList.get(i)._2+old._2);
					resultmap.put(key, newval);
				}
				
			}
			
			JavaPairRDD<String, Integer> xx = jsc.parallelize(topList).mapToPair((f)-> new Tuple2(f._1, f._2));
			xx.saveAsHadoopFile("hello", String.class, Integer.class, RDDMultipleTextOutputFormat.class);
			
			
			
			List<Tuple2<String, Integer>> result =  new ArrayList<Tuple2<String, Integer>>(resultmap.values());
			result.sort((a,b) -> b._2.compareTo(a._2));
			for(Tuple2<String, Integer> list : result){
						System.out.println(String.format("%s (%s tweets)", list._1, list._2));
				}
			
			return null;
	    	
	    });
	    */
	    ssc.start();
	    
	    ssc.awaitTermination();


	}

}