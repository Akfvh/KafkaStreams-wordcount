package com.dcclab.examples.streams.wordcount;


import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

public class WordCountExample {

    public static void main(String[] args) throws Exception{

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream_wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "163.239.14.92:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

	/* implement here */

		// topology builder
		final StreamsBuilder builder = new StreamsBuilder();

		/* source stream */
		// input Topic name from lassServer, 
		// /home/lass/hibench/Hibench/conf/worklaods/streaming/wordcount.onf
		KStream<String, String> lines = builder.stream("wordcount1");

		// stream processor
		KTable<String, Long> wordCounts = lines
			.flatMapValues(line -> {
				String[] words = line.split(",");
				return Arrays.asList(words).subList(0, 1);
			})
			.selectKey((key, word) -> word)
			.groupByKey()
			.count();

		/* output stream */
		// output Topic name from localhost,
		// ~/Assignment/kafkaconsumer_test/src/main/java/com/ ... /consumer/consumer.java
		wordCounts.toStream().to("wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();

		Runtime.getRuntime().addShutdownHook(
			new Thread(streams::close));
		while(true){
			System.out.println(streams.toString());
			try{
				Thread.sleep(5000);
			}
			catch(InterruptedException e){
				break;
			}
		}
	}
}
