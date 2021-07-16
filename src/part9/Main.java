package part9;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;


public class Main {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Bai_9").master("local").getOrCreate();
		Dataset<Row> data = spark.read().option("header", true).option("inferSchema", true).csv("src/part9/input.csv");
		
		Dataset<Row> result = data.withColumn("concat_Qid", functions.concat(functions.lit("Qid_"), data.col("Qid")))
				.groupBy("ParticipantID", "Assessment", "GeoTag").pivot("concat_Qid").agg(functions.first("AnswerText"));
		
		result.show();
	}
}
