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
		
		data.createOrReplaceTempView("data");
		
		WindowSpec wins = Window.partitionBy("department").orderBy("department");
		Dataset<Row> result_1 = data.withColumn("running_total", functions.row_number().over(wins));
		
		result_1.createOrReplaceTempView("result_1");
		Dataset<Row> result = spark.sql("select time, department, items_sold, running_total, sum(items_sold) "
				+ "from result_1 group by time, department, items_sold, running_total "
				+ "order by department, running_total ").drop("running_total");
		
		result.show();
	}
}
