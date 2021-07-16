package part4;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

public class Main {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Bai_4").master("local").getOrCreate();
		Dataset<Row> data = spark.read().option("header", true).option("inferSchema", true).csv("src/part4/input.csv");
				
		WindowSpec wins = Window.partitionBy("department").orderBy("time").rowsBetween(Window.unboundedPreceding(), Window.currentRow());
		Dataset<Row> result = data.withColumn("running_total", functions.sum("items_sold").over(wins));
		
		result.show();
	}
}
