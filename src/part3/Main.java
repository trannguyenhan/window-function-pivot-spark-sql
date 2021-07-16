package part3;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;

public class Main {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Bai_1").master("local").getOrCreate();
		
		StructType schema = new StructType().add("id", "integer")
				.add("day", "integer")
				.add("price", "integer")
				.add("units", "integer");

        List<Row> listOfdata = new ArrayList<Row>();
        listOfdata.add(RowFactory.create(100,1,23,10));
        listOfdata.add(RowFactory.create(100,2,45,11));
        listOfdata.add(RowFactory.create(100,3,67,12));
        listOfdata.add(RowFactory.create(100,4,78,13));
        listOfdata.add(RowFactory.create(101,1,23,10));
        listOfdata.add(RowFactory.create(101,2,45,13));
        listOfdata.add(RowFactory.create(101,3,67,14));
        listOfdata.add(RowFactory.create(101,4,78,15));
        listOfdata.add(RowFactory.create(102,1,23,10));
        listOfdata.add(RowFactory.create(102,2,45,11));
        listOfdata.add(RowFactory.create(102,3,67,16));
        listOfdata.add(RowFactory.create(102,4,78,18));
        
        Dataset<Row> data = spark.createDataFrame(listOfdata,schema);
        
        Dataset<Row> result_1 = data.withColumn("concat_day", functions.concat(functions.lit("price_"), data.col("day")))
				.groupBy("id").pivot("concat_day").agg(functions.first("price"));
        
        Dataset<Row> result_2 = data.withColumn("concat_day", functions.concat(functions.lit("unit_"), data.col("day")))
				.groupBy("id").pivot("concat_day").agg(functions.first("units"));
        
        Dataset<Row> result = result_1.join(result_2, result_1.col("id").$eq$eq$eq(result_2.col("id"))).drop(result_2.col("id")).sort("id");
        result.show();
	}
}
