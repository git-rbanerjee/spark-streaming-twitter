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
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;
import scala.reflect.macros.internal.macroImpl;



public class NaiveBaysTest2 {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("NaiveBaysTest").setMaster("local[*]");
		
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		
		HashingTF htf  = new HashingTF(10000);
		NaiveBayesModel model = NaiveBayesModel.load(jsc.sc(), "nb-model");
		double iii = model.predict(htf.transform(Arrays.asList("So much reason to be grateful... The airtel northern region was more than a standard".split(" "))));
		System.out.println(iii);
		/*MulticlassMetrics metrics = new MulticlassMetrics(JavaRDD.toRDD(predictionAndLabels));
		System.out.println(metrics.fMeasure(0.0));
		System.out.println(metrics.fMeasure(1.0));*/
	}

}
