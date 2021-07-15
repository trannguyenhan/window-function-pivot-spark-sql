package part1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Bai_1").master("local").getOrCreate();
		Dataset<Row> data = spark.read().option("header", true).option("inferSchema", true).csv("src/part1/input.csv");
		
		data.createOrReplaceTempView("data");
		Dataset<Row> result = spark.sql("select population from data order by population desc limit 1");
		
		
		result.show();
	}
}
