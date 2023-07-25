package com.dcclab.examples.streams.consumer;


import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) throws Exception{

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "163.239.14.92:9092");
	//props.put("group.id", "karim-group-id-1"); 
	props.put("group.id", "test-consumer-group");
	props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer"); // value deserializer

	KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(props);
	consumer.subscribe(Collections.singletonList("wordcount-output"));
	
	String message = null;
	String key = null;
        try {
            while(true) {
                ConsumerRecords<String, Long> records = consumer.poll(0);

                for (ConsumerRecord<String, Long> record : records) {
                    message = record.value().toString();
		    key = record.key();
                    System.out.println(key + "------- " + message);
                }
            }
        } catch(Exception e) {
		System.out.println("Exception consumung");
        } finally {
            consumer.close();
        }
    }
}
