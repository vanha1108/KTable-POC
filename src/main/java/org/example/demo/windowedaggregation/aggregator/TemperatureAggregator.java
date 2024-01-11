package org.example.demo.windowedaggregation.aggregator;

import org.apache.kafka.streams.kstream.Aggregator;
import org.example.demo.windowedaggregation.TemperatureAggregate;
import org.example.demo.windowedaggregation.TemperatureReading;

public class TemperatureAggregator implements Aggregator<String, TemperatureReading, TemperatureAggregate> {

    @Override
    public TemperatureAggregate apply(String key, TemperatureReading value, TemperatureAggregate aggregate) {
        aggregate.addTemperature(value.getTemperature());
        return aggregate;
    }
}
