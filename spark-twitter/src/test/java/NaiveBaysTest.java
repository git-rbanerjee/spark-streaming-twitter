import java.util.Arrays;




import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;
import scala.reflect.macros.internal.macroImpl;



public class NaiveBaysTest {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("NaiveBaysTest").setMaster("local[*]");
		
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		
		HashingTF htf  = new HashingTF(10000);
		JavaRDD<String> dataSource = jsc.textFile("ds1.csv");
		JavaRDD<LabeledPoint> allWords = dataSource.map((f) -> {
			String[] parts = f.split(",");
			return new LabeledPoint(Double.parseDouble(parts[1]), htf.transform(Arrays.asList(parts[2].split(" "))));
		});
		
		
				
		JavaRDD<LabeledPoint>[] xx = allWords.randomSplit(new double[]{0.8,0.2}, 11L);
		JavaRDD<LabeledPoint> training = xx[0];
		JavaRDD<LabeledPoint> test = xx[1];
		
		NaiveBayesModel model = NaiveBayes.train(JavaRDD.toRDD(training));
		
		JavaRDD<Tuple2<Object, Object>> predictionAndLabels  = test.map((f) ->
		{
			
			Double score = model.predict(f.features());
			return new Tuple2<Object, Object>(score, f.label());
		});
		/*dataSource.foreach((f) -> {
			double iii = model.predict(htf.transform(Arrays.asList("sad unhappy sad")));
			
		});*/
		double iii = model.predict(htf.transform(Arrays.asList("happy so hapy".split(" "))));
		System.out.println(iii);
		MulticlassMetrics metrics = new MulticlassMetrics(JavaRDD.toRDD(predictionAndLabels));
		System.out.println(metrics.fMeasure(0.0));
		System.out.println(metrics.fMeasure(1.0));
	}

}
