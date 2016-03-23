import java.util.ArrayList;
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



public class NaiveBaysTest3 {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("NaiveBaysTest").setMaster("local[*]");
		
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		
		HashingTF htf  = new HashingTF(10000);
		JavaRDD<String> dataSource = jsc.textFile("ds1.csv");
		JavaRDD<Double> labels = dataSource.map((f) -> {
			
			return Double.parseDouble(f.split(",")[1]);
		});
		JavaRDD<Vector> dataTF = dataSource.map((f) -> {
			List<String> nGrams = new ArrayList<>();
			String[] tokens = f.split(" ");
			int i=0;
			for(i=0; i< tokens.length - 2 ;i++)
			{
				nGrams.add(tokens[i]);
				nGrams.add(tokens[i]+" "+tokens[i+1]);
				nGrams.add(tokens[i]+" "+tokens[i+1]+" "+tokens[i+2]);
			}
			nGrams.add(tokens[i]);
			//nGrams.add(tokens[i]+" "+tokens[i+1]);
			
			
			return htf.transform(nGrams);
		} );
		
		IDFModel idf = new IDF(2).fit(dataTF);
		JavaRDD<Vector> dataTFIDF = idf.transform(dataTF);
		
		JavaRDD<LabeledPoint> labeledPoints = labels.zip(dataTFIDF).map((f) -> {
			return new LabeledPoint(f._1, f._2);
		});
		
				
		JavaRDD<LabeledPoint>[] xx = labeledPoints.randomSplit(new double[]{0.6,0.4}, 11L);
		JavaRDD<LabeledPoint> training = xx[0];
		JavaRDD<LabeledPoint> test = xx[1];
		
		NaiveBayesModel model = NaiveBayes.train(JavaRDD.toRDD(training),1.0);
		
		//model.save(jsc.sc(), "nb-model");
		JavaRDD<Tuple2<Object, Object>> predictionAndLabels  = test.map((f) ->
		{
			
			Double score = model.predict(f.features());
			return new Tuple2<Object, Object>(score, f.label());
		});
		/*dataSource.foreach((f) -> {
			double iii = model.predict(htf.transform(Arrays.asList("sad unhappy sad")));
			
		});*/
		//double iii = model.predict(idf.transform(htf.transform(Arrays.asList("So much reason to be grateful... The airtel northern region was more than a standard".split(" ")))));
		
		
		//System.out.println(iii);
		MulticlassMetrics metrics = new MulticlassMetrics(JavaRDD.toRDD(predictionAndLabels));
		for(double dd: metrics.labels())
			System.out.println(metrics.fMeasure(dd));

		
		/*long countTrain = test.mapToPair((p) -> {
			return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
		}).filter((f) -> f._1.equals(f._2)).count();
		System.out.println( countTrain );
		System.out.println(test.count());*/
		
		
	}

}
