package part7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;

public class Main {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Bai_7").master("local").getOrCreate();
		Dataset<Row> data = spark.read().option("header", true).option("inferSchema", true).csv("src/part7/input.csv");
		data = data.sort(data.col("Salary").desc());
		data = data.withColumn("stt", functions.rank().over(Window.orderBy("Salary")));
		data = data.withColumn("Salary_count", functions.count("Salary").over()).sort(data.col("Salary").desc());
		data.createOrReplaceTempView("data");
		
		Dataset<Row> data_1 = spark.sql("select Employee, Salary, stt / Salary_count as per from data");
		data_1.createOrReplaceTempView("data_1");
		
		Dataset<Row> result_1 = spark.sql("select Employee, Salary from data_1 where per > 0.7 and per <= 1");
		result_1 = result_1.withColumn("Percentage", functions.lit("High"));
		
		Dataset<Row> result_2 = spark.sql("select Employee, Salary from data_1 where per > 0.3 and per <= 0.7");
		result_2 = result_2.withColumn("Percentage", functions.lit("Average"));
		
		Dataset<Row> result_3 = spark.sql("select Employee, Salary from data_1 where per <= 0.3 ");
		result_3 = result_3.withColumn("Percentage", functions.lit("Low"));
		
		Dataset<Row> result = result_1.union(result_2).union(result_3);
		result.show();
	}
}
