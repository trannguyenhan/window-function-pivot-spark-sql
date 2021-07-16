package part5;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

public class Main {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Bai_5").master("local").getOrCreate();
		Dataset<Row> data = spark.read().option("header", true).option("inferSchema", true).csv("src/part5/input.csv");
		
		WindowSpec wins = Window.partitionBy("department").orderBy("time").rowsBetween(Window.currentRow() - 1, Window.currentRow());
		Dataset<Row> result_1 = data.withColumn("diff_sum", functions.sum("running_total").over(wins));
		Dataset<Row> result_2 = result_1.withColumn("diff_1", functions.max("running_total").over(wins));
		result_2.createOrReplaceTempView("data_1");
		
		spark.sql("select time, department, items_sold, running_total, diff_1, diff_sum - diff_1 as diff_2 from data_1")
				.createOrReplaceTempView("data_2");
		
		Dataset<Row> result = spark.sql("select time, department, items_sold, running_total, diff_1 - diff_2 as diff from data_2");
		result.show();
	}
}
