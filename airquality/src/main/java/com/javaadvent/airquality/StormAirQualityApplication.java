package com.javaadvent.airquality;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class StormAirQualityApplication {

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: application <comma_separated_values>");
			return;
		}

		String[] numbers = args[0].split(",");
		System.out.println("Measuring for air quality values: " + args[0]);
		new StormAirQualityApplication().countPollutedRegions(numbers);
	}

	public void countPollutedRegions(String[] numbers) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("numbers-spout", new StormAirQualitySpout(numbers));
		builder.setBolt("number-bolt", new StormAirQualityBolt()).
			shuffleGrouping("numbers-spout");
		
		Config conf = new Config();
		conf.setDebug(true);
		LocalCluster localCluster = null;
		try {
			localCluster = new LocalCluster();
			localCluster.submitTopology("airquality-topology", 
					conf, builder.createTopology());
			Thread.sleep(10000);
			localCluster.shutdown();
		} catch (InterruptedException ex) {
			localCluster.shutdown();
		}
	}
}
