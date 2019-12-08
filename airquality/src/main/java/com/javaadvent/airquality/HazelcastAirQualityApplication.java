package com.javaadvent.airquality;

import java.util.Arrays;
import java.util.List;

import com.hazelcast.core.IList;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

public class HazelcastAirQualityApplication {

	private static final int HIGH_THRESHOLD = 10;

	public static void main(String[] args) {

		if (args.length < 1) {
			System.err.println("Usage: application <comma_separated_values>");
			return;
		}

		String[] numbers = args[0].split(",");
		System.out.println("Measuring for air quality values: " + args[0]);
		new HazelcastAirQualityApplication().countPollutedRegions(numbers);
	}

	public long countPollutedRegions(String[] numbers) {

		Pipeline p = Pipeline.create();
		p.drawFrom(Sources.list("numbers")).
			map(number -> Integer.valueOf((String) number))
			.filter(number -> number > HIGH_THRESHOLD).drainTo(Sinks.list("filteredNumbers"));

		JetInstance jet = Jet.newJetInstance();
		IList<String> numbersList = jet.getList("numbers");
		numbersList.addAll(Arrays.asList(numbers));

		try {
			jet.newJob(p).join();

			List<String> filteredRecordsList = jet.getList("filteredNumbers");
			int pollutedRegions = filteredRecordsList.size();
			System.out.println("Number of severely polluted regions: " + pollutedRegions);

			return pollutedRegions;
		} finally {
			Jet.shutdownAll();
		}
	}
}
