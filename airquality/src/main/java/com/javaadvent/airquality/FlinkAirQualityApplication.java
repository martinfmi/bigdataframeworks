package com.javaadvent.airquality;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkAirQualityApplication {

	private static final int HIGH_THRESHOLD = 10;

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: application <comma_separated_values>");
			return;
		}

		String[] numbers = args[0].split(",");
		System.out.println("Measuring for air quality values: " + args[0]);
		new FlinkAirQualityApplication().countPollutedRegions(numbers);
	}

	public long countPollutedRegions(String[] numbers) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.
				createLocalEnvironment();

		DataStream<Integer> stream = env.fromCollection(Arrays.asList(numbers)).
				map(number -> Integer.valueOf(number))
				.filter(number -> number > HIGH_THRESHOLD).returns(Integer.class);
		long pollutedRegions = 0;
		Iterator<Integer> numbersIterator = DataStreamUtils.collect(stream);
		while(numbersIterator.hasNext()) {
			pollutedRegions++;
			numbersIterator.next();
		}
		System.out.println("Number of severely polluted regions: " + pollutedRegions);
		return pollutedRegions;
	}

}
