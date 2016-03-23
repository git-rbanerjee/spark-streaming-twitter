

import java.util.Arrays;
import java.util.regex.Pattern;

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

import com.rabin.spark.twitter.MySoundex;

public class TwitterPopularTag2 {
	
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
	    JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(ssc);
	    
	    JavaDStream<String> english = stream.filter((f)->f.getUser().getLang().equals("en"))
	    		.map((status)-> status.getText().replaceAll("[^\\x00-\\x7F]", "").replace("\n", " "));
	    
	    JavaDStream<String> words = english.flatMap( (status) -> Arrays.asList(SPACE.split(status)));
	    
	    
	   //english.print();
	   JavaDStream<String> hashTags = words.filter((s) -> s.startsWith("#") && s.length()>3);
	   MySoundex sx = new MySoundex();
	    JavaPairDStream<String, Tuple2<String, Integer>> hashTagsOnes = hashTags.mapToPair((s) -> new Tuple2<String, Tuple2<String, Integer>>(sx.encode(s), new Tuple2<String, Integer>(s, 1)));
	    
	    JavaPairDStream<String, Tuple2<String, Integer>> topCount60 = hashTagsOnes.reduceByKeyAndWindow((a,b) -> new Tuple2(a._1+b._1, a._2+b._2), Durations.minutes(10));
	    
	    JavaPairDStream<String, Integer> topCount60Sorted = topCount60.mapToPair((f) -> new Tuple2<String,Integer>(f._2._1, f._2._2)).mapToPair((s) -> s.swap())
	    														.transformToPair((rdd) -> rdd.sortByKey(false))
	    															.mapToPair((s) -> s.swap());
	    
	    
	    topCount60Sorted.print(10);
	    /*topCount60Sorted.foreachRDD( (rdd) -> {
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
	    	
	    });*/
	    
	    
	    ssc.start();
	    ssc.awaitTermination();


	}

}