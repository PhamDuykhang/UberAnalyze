package vn.ctu.cit.lv.uberanalyze.traimodel;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkProsess {
	private static final  String APP_NAME="ProcessData";
//	private static final String CSV_FILE_PATH = "D:/UberAnalyzeGPS/RS/sim/*";
	private static final String CSV_FILE_PATH2 = "D:/UberAnalyzeGPS/RS/sim/uber-raw-data-sep14.csv";
	public static void main(String[] args) {
		
		SparkConf sparkconf= new SparkConf()
				.setAppName(APP_NAME)
				.setMaster("local[3]");
		SparkContext sc = new SparkContext(sparkconf);
		SparkSession sparksession = new SparkSession(sc);
		StructType uberDataSchema = new StructType(new StructField[] {
				new StructField("dt", DataTypes.TimestampType, true,Metadata.empty()),
				new StructField("lat", DataTypes.DoubleType, true,Metadata.empty()),
				new StructField("lon", DataTypes.DoubleType, true,Metadata.empty()),
				new StructField("base", DataTypes.StringType, true,Metadata.empty())
		});
		Dataset<Row> Uber =sparksession.readStream().schema(uberDataSchema).csv(CSV_FILE_PATH2);
		StreamingQuery qr= Uber.writeStream()
				.outputMode("update")
				.outputMode("console")
				.start();
		try {
			qr.awaitTermination();
		} catch (StreamingQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
}
