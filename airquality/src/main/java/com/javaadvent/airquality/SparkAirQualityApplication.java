package com.javaadvent.airquality;

import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class SparkAirQualityApplication {

	private static final int HIGH_THRESHOLD = 10;

	public static void main(String[] args) {

		if (args.length < 1) {
			System.err.println("Usage: application <comma_separated_values>");
			return;
		}

		String[] numbers = args[0].split(",");
		System.out.println("Measuring for air quality values: " + args[0]);
		new SparkAirQualityApplication().countPollutedRegions(numbers);
	}

	public long countPollutedRegions(String[] numbers) {
		SparkSession session = SparkSession.builder().
				appName("AirQuality").
				master("local[4]").
				getOrCreate();
		Dataset<String> numbersSet = session.createDataset(Arrays.asList(numbers), 
				Encoders.STRING());
		long pollutedRegions = numbersSet.map(number -> Integer.valueOf(number), 
				Encoders.INT())
				.filter(number -> number > HIGH_THRESHOLD).count();
		
		System.out.println("Number of severely polluted regions: " + pollutedRegions);
		return pollutedRegions;
	}
}
