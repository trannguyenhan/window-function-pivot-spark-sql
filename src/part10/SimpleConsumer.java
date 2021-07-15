package part10;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SimpleConsumer {
	public static void main(String[] args) {
		Properties prop = new Properties();
		
		prop.put("bootstrap.servers", "localhost:9092");
		prop.setProperty("group.id", "test");
	    prop.setProperty("enable.auto.commit", "true");
	    prop.setProperty("auto.commit.interval.ms", "1000");

		prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		try (Consumer<String, String> consumer = new KafkaConsumer<String, String>(prop)) {
			consumer.subscribe(Arrays.asList("topic1"));
			while(true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				int index = 0;
				int size = records.count();
				List<String> list = new ArrayList<String>();
				for(ConsumerRecord<String, String> record: records) {
					if(index == 0) {
						System.out.println("Oldest message: ");
						System.out.println("offset = " + ", key = " + record.key() + ", " + record.offset() + ", value = " + record.value());
						System.out.println("|---------|");
					}
					
					if(index == size-1) {
						System.out.println("Newest message: ");
						System.out.println("offset = " + ", key = " + record.key() + ", " + record.offset() + ", value = " + record.value());
						System.out.println("|---------|");
					}
					
					list.add(record.value());
					index++;
				}
				
				list.forEach(s -> System.out.println(s + "\n----------------"));
				index = 0;
			}
		}
	}

}
