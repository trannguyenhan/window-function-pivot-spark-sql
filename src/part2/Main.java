package part2;

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
				.add("group", "integer");

        List<Row> listOfdata = new ArrayList<Row>();
        listOfdata.add(RowFactory.create(0,0));
        listOfdata.add(RowFactory.create(1,1));
        listOfdata.add(RowFactory.create(2,0));
        listOfdata.add(RowFactory.create(3,1));
        listOfdata.add(RowFactory.create(4,0));
        
        Dataset<Row> data = spark.createDataFrame(listOfdata,schema);
        
        Dataset<Row> result = data.groupBy("group")
        		.agg(functions.collect_list("id").as("ids")).sort("group");
        
        result.show();
	}
}
 