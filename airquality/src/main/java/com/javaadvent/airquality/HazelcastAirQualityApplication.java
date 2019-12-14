package com.javaadvent.airquality;

import java.util.List;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;

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
		p.drawFrom(TestSources.items(numbers))
				.map(Integer::valueOf)
				.filter(number -> number > HIGH_THRESHOLD)
				.aggregate(AggregateOperations.counting())
				.drainTo(Sinks.list("pollutedRegions"));

		JetInstance jet = Jet.newJetInstance();

		try {
			jet.newJob(p).join();

			List<Long> filteredRecordsList = jet.getList("pollutedRegions");
			long pollutedRegions = filteredRecordsList.get(0);
			System.out.println("Number of severely polluted regions: " + pollutedRegions);

			return pollutedRegions;
		} finally {
			Jet.shutdownAll();
		}
	}
}
