package part8;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Bai_8").master("local").getOrCreate();
		Dataset<Row> data = spark.read().option("header", true).option("inferSchema", true).csv("src/part8/input.csv");
		
		data.createOrReplaceTempView("data");
		spark.sql("SELECT\n"
				+ "  id,\n"
				+ "  title,\n"
				+ "  genre,\n"
				+ "  quantity,\n"
				+ "  rank\n"
				+ "FROM (\n"
				+ "  SELECT\n"
				+ "    id,\n"
				+ "    title,\n"
				+ "    genre,\n"
				+ "    quantity,\n"
				+ "    dense_rank() OVER (PARTITION BY genre ORDER BY quantity DESC) as rank\n"
				+ "  FROM data) tmp\n"
				+ "WHERE\n"
				+ "  rank <= 2").show();
		
	}
}
