package com.javaadvent.airquality;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

public class KafkaStreamsAirQualityApplication {

	private static final int HIGH_THRESHOLD = 10;

	public static void main(String[] args) {

		new KafkaStreamsAirQualityApplication().countPollutedRegions();
	}
	
	public long countPollutedRegions() {

		List<String> result = new LinkedList<String>();
		final StreamsBuilder builder = new StreamsBuilder();

		final Serde<String> stringSerde = Serdes.String();
		builder.stream("numbers", Consumed.with(stringSerde, stringSerde))
				.map((key, value) -> new KeyValue<>(key, Integer.valueOf(value))).
					filter((key, value) -> value > HIGH_THRESHOLD)
				.foreach((key, value) -> {
					result.add(value.toString());
				});

		final Topology topology = builder.build();
		final KafkaStreams streams = new KafkaStreams(topology, 
				createKafkaStreamsConfiguration());
		streams.start();

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		int pollutedRegions = result.size();
		System.out.println("Number of severely polluted regions: " + pollutedRegions);
		streams.close();
		return pollutedRegions;
	}

	private Properties createKafkaStreamsConfiguration() {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "text-search-config");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		return props;
	}
	
}
