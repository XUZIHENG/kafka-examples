package examples;/**
 * Created by Administrator on 2017/9/19.
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Arrays;
import java.util.Properties;

/**
 * @date 2017/9/19
 * @time 21:05
 */
public class Consumer {


	private  String topicName;//消费的主题

	public Consumer(String topic){

		topicName = topic;

	}

	public  void consume(){
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.0.83:9092,192.168.0.84:9092");//kafka 集群IP
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topicName));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records)
				System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
		}

	}
	public static void main(String[] args) {
		Consumer c = new Consumer("test-topic");
		c.consume();
	}
}
