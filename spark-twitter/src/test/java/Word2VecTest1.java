import java.util.Arrays;
import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;

import scala.Tuple2;



public class Word2VecTest1 {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("NaiveBaysTest").setMaster("local[*]");
		
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		
		
		JavaRDD<String> dataSource = jsc.textFile("ds1.csv");
		JavaRDD<Double> labels = dataSource.map((f) -> {
			
			return Double.parseDouble(f.split(",")[1]);
		});
		
		
		
		Word2Vec wtv = new Word2Vec();
		
		Word2VecModel wtvM = wtv.fit(dataSource.map((f) -> Arrays.asList(f.split(",")[3].split(" "))));
		
		
		
		
	}

}
