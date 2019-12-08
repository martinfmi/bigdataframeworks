package com.javaadvent.airquality;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class StormAirQualityBolt extends BaseRichBolt {

	private static final int HIGH_THRESHOLD = 10;

	private int pollutedRegions = 0;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("number"));
	}

	@Override
	public void prepare(Map<String, Object> params, TopologyContext context, OutputCollector collector) {
	}

	@Override
	public void execute(Tuple tuple) {
		String number = tuple.getStringByField("number");
		Integer numberInt = Integer.valueOf(number);
		if (numberInt > HIGH_THRESHOLD) {
			pollutedRegions++;
		}
	}

	@Override
	public void cleanup() {
		super.cleanup();
		System.out.println("Number of severely polluted regions: " + pollutedRegions);
	}
	
}
