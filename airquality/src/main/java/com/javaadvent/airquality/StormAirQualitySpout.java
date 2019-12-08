package com.javaadvent.airquality;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class StormAirQualitySpout extends BaseRichSpout {

	private boolean emitted = false;

	private SpoutOutputCollector collector;

	private String[] numbers;

	public StormAirQualitySpout(String[] numbers) {
		this.numbers = numbers;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("number"));
	}

	@Override
	public void open(Map<String, Object> paramas, 
			TopologyContext context, 
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		if(!emitted) {
			for(String number : numbers) {
				collector.emit(new Values(number));
			}
			emitted = true;
		}
	}
}
