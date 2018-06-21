package vn.ctu.cit.lv.uberanalyze.traimodel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class CreateKmeensModel {

	static final  String APP_NAME="Creatdatamodel";
	public static void main(String[] args) {
		
		// TODO Auto-generated method stub
		
		SparkConf sparkconf= new SparkConf()
				.setAppName(APP_NAME)
				.setMaster("local[3]");
		SparkContext sc = new SparkContext(sparkconf);
		SparkSession sparksession = new SparkSession(sc);
//				.Builder().config(sparkconf).getOrCreate();
		StructType uberDataSchema = new StructType(new StructField[] {
				new StructField("dt", DataTypes.TimestampType, true,Metadata.empty()),
				new StructField("lat", DataTypes.DoubleType, true,Metadata.empty()),
				new StructField("lon", DataTypes.DoubleType, true,Metadata.empty()),
				new StructField("base", DataTypes.StringType, true,Metadata.empty())
		});
		Dataset<Row> uberData= sparksession.read().schema(uberDataSchema)
				.option("header", "false")
				.option("mode", "FAILFAST")
				.csv("D:/UberAnalyzeGPS/RS/uber-raw-data-aug14.csv");
		uberData.printSchema();
		uberData.cache();
//		uberData.show();
		String[] listcol = {"lat","lon"};
		VectorAssembler vectorAssembler = new VectorAssembler()
				.setInputCols(listcol).setOutputCol("Features");
		Dataset<Row> uberDataFeatures = vectorAssembler.transform(uberData);
		double[] weightslipt = {0.7,0.3};
		Dataset<Row>[] dataslipt=  uberDataFeatures.randomSplit(weightslipt);
		KMeans kmeans= new KMeans()
				.setFeaturesCol("Features")
				.setK(8)
				.setPredictionCol("Predict");
		KMeansModel model = kmeans.fit(dataslipt[0]);

		Vector[] centerclust = model.clusterCenters();
		for(Vector a: centerclust) {
			System.out.println(a.toString());
		}
		Dataset<Row> predict =model.transform(dataslipt[1]);
		System.out.println("Predict result");
		predict.show();
		try {
			model.save("D:/UberAnalyzeGPS/RS/modelKmeans");
		} catch (IOException e) {
			// TODO Auto-generated catch block
		
			
		}
	
		sparksession.close();
		
	}

}
