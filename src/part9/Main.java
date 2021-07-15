package part9;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import part2.Part2;
import scala.Function1;

public class Main {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Bai_9").master("local").getOrCreate();
		Dataset<Row> data = spark.read().option("header", true).option("inferSchema", true).csv("src/part9/input.csv");
		
		Dataset<Row> result = data.groupBy("ParticipantID", "Assessment", "GeoTag")
        		.agg(functions.collect_list("AnswerText").as("Qids")).sort("ParticipantID");
		
		result = result.withColumn("Qid_1", functions.lit("null"));
		result = result.withColumn("Qid_2", functions.lit("null"));
		result = result.withColumn("Qid_3", functions.lit("null"));
		
//		Encoder<Part2> partEncoder = Encoders.bean(Part2.class);
//		Dataset<Part2> resultPart2 = result.map(new Function1<Row, Part2>() {
//			@Override
//			public Part2 apply(Row row) {
//				List<String> listString = row.getList(3);
//				if(listString.size() < 3) {
//					listString.add("null");
//				}
//				
//				Part2 part2 = new Part2();
//				part2.setParticipantID(row.getString(0));
//				part2.setAssessment(row.getString(1));
//				part2.setGeoTag(row.getString(2));
//				part2.setQid_1(listString.get(0));
//				part2.setQid_2(listString.get(1));
//				part2.setQid_3(listString.get(2));
//				
//				return part2;
//			}
//		}, partEncoder);
//		
//		
//		resultPart2.show();
		
		result.show();
	}
}
