package org.example.demo.windowedaggregation;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.example.demo.materializedviewmaintenance.serdes.JsonSerde;
import org.example.demo.windowedaggregation.serdes.TemperatureAggregateSerde;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

public class WindowedAggregationsDemo {

    public static void main(String[] args) throws IOException {
        runPipelineWindowedAggregations();
    }

    private static void runPipelineWindowedAggregations() throws IOException {
        // Set up Kafka Streams properties
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream("src/main/resources/application.properties")) {
            props.load(in);
        }
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowed-aggregations-test-32");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TemperatureAggregateSerde.class.getName());

        // Use JsonSerde for TemperatureReading
        JsonSerde<TemperatureReading> temperatureReadingSerde = new JsonSerde<>(TemperatureReading.class);

        // Create a StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();

        // Create a KStream from the "temperature-topic"
        KStream<String, TemperatureReading> temperatureStream = builder.stream(
                "temperature-topic-2",
                Consumed.with(Serdes.String(), temperatureReadingSerde)
        );

        // Aggregate average temperature for each hour using tumbling window
        TimeWindowedKStream<String, TemperatureReading> windowedStream = temperatureStream
                .groupBy((sensorId, temperatureReading) -> sensorId, Grouped.with(Serdes.String(), temperatureReadingSerde))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)));

        // Aggregate and calculate average temperature
        KStream<Windowed<String>, TemperatureAggregate> windowedTemperatureAggregateKStream = windowedStream
                .aggregate(
                        TemperatureAggregate::new,
                        (sensorId, temperatureReading, aggregate) -> {
                            aggregate.addTemperature(temperatureReading.getTemperature());
                            return aggregate;
                        }
                )
                .toStream();

//        windowedTemperatureAggregateKStream.peek((key, value) -> System.out.println("Key: " + key + " value: " + value));

        windowedTemperatureAggregateKStream.map(((key, value) -> new KeyValue<>(key.key(), value.showAverageTemperature())))
                .peek(((key, value) -> System.out.println("Key: " + key + " value: " + value)))
                .mapValues(value -> String.valueOf(value))
                .to("average-temperature-topic", Produced.with(Serdes.String(), Serdes.String()));

        // Build the topology
        Topology topology = builder.build();

        // Create and start Kafka Streams application
        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaStreams.close();
        }));
    }
}
