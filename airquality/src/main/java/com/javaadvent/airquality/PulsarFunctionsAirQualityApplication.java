package com.javaadvent.airquality;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

public class PulsarFunctionsAirQualityApplication 
	implements Function<String, Void> {

	private static final int HIGH_THRESHOLD = 10;

    @Override
    public Void process(String input, Context context) throws Exception {
    	
    	int number = Integer.valueOf(input);
    	
    	if(number > HIGH_THRESHOLD) {
            context.incrCounter("pollutedRegions", 1);
    	}
        return null;
    }
}

