package examples;/**
 * Created by Administrator on 2017/9/19.
 */

import org.apache.kafka.clients.producer.*;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * @date 2017/9/19
 * @time 20:19
 */
public class Producer {

	private String topicName;

	public Producer(String name){
		topicName = name;
	}

	public void sendRecords() throws IOException, InterruptedException {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.0.83:9092,192.168.0.84:9092");//kafka 集群IP
		props.put("acks", "1");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(props);
		int i = 0;
		String record = "1";//发送的内容
		//循环发送一百次
		while(i<100) {
			producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), record), new Callback() {
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if (e != null)
						e.printStackTrace();
					System.out.println("The offset of the record we just sent is: " + metadata.offset());
				}
			});
			i++;
		}
		producer.close();
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		new Producer("test-topic").sendRecords();
	}

}
