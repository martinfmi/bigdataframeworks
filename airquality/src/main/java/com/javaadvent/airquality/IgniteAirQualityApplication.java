package com.javaadvent.airquality;

import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class IgniteAirQualityApplication {

	private static final int HIGH_THRESHOLD = 10;
	private static final String NUMBERS_CACHE = "NUMBERS_CACHE";

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: application <comma_separated_values>");
			return;
		}

		String[] numbers = args[0].split(",");
		System.out.println("Measuring for air quality values: " + args[0]);
		new IgniteAirQualityApplication().countPollutedRegions(numbers);
	}

	public long countPollutedRegions(String[] numbers) {

		IgniteConfiguration igniteConfig = new IgniteConfiguration();
		CacheConfiguration<String, Long> cacheConfig = 
				new CacheConfiguration<String, Long>();
		cacheConfig.setIndexedTypes(Integer.class, String.class);

		cacheConfig.setName(NUMBERS_CACHE);
		igniteConfig.setCacheConfiguration(cacheConfig);
		
		try (Ignite ignite = Ignition.start(igniteConfig)) {
			IgniteCache<String, String> cache = ignite.getOrCreateCache(NUMBERS_CACHE);
			try (IgniteDataStreamer<Integer, String> streamer = 
					ignite.dataStreamer(cache.getName())) {
				int key = 0;
				for (String number : numbers) {
					streamer.addData(key++, number);
				}
			}

			SqlFieldsQuery query = new SqlFieldsQuery("select * from String where _val > " 
					+ HIGH_THRESHOLD);
			
			FieldsQueryCursor<List<?>> cursor = cache.query(query);

			int pollutedRegions = cursor.getAll().size();
			System.out.println("Number of severely polluted regions: " + pollutedRegions);
			return pollutedRegions;
		}
	}

}
