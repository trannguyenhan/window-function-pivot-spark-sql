package part3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
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
        
        Dataset<Row> result_1 = data.groupBy("id")
        		.agg(functions.collect_list("price").as("prices"));
        
//        Dataset<Row> result_2 = data.groupBy("id")
//        		.agg(functions.collect_list("unit").as("units"));
        
//        result_1.flatMap(new FlatMapFunction<Row, Row>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Iterator<Row> call(Row row) throws Exception {
//				List<Integer> list = row.getList(1);
//				
//				List<Row> listTmpRow = new ArrayList<Row>();
//				
//				
//				return null;
//			}
//
//		}, null);
        
        result_1.show();
//        result_2.show();
	}
}
