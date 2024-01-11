package org.example.demo.alertingandmonitoring;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.example.demo.materializedviewmaintenance.serdes.JsonSerde;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

public class ErrorRateMonitoringDemo {

    public static void main(String[] args) throws IOException {
        runPipelineErrorRateMonitoring();
    }

    private static void runPipelineErrorRateMonitoring() throws IOException {
        // Set up Kafka Streams properties
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream("src/main/resources/application.properties")) {
            props.load(in);
        }
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "error-monitoring-test-6");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Use JsonSerde for ErrorEvent
        JsonSerde<ErrorEvent> errorEventSerde = new JsonSerde<>(ErrorEvent.class);

        // Create a StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();

        // Create a KStream from the "error-event-topic"
        KStream<String, ErrorEvent> errorEventStream = builder.stream(
                "error-event-topic",
                Consumed.with(Serdes.String(), errorEventSerde)
        );

        // Calculate error rate over a sliding window
        KTable<Windowed<String>, Long> countTable = errorEventStream
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofMinutes(3))) // sliding window
                .count();

        countTable.toStream().foreach((key, value) -> {
            System.out.println("Count Table: Key: " + key + ", Value: " + value);
        });

        KStream<Windowed<String>, Long> resultStream = countTable.toStream()
                .filter((key, count) -> count >= 10);// Trigger alert if error count exceeds 10 in the window

        // Replace trigger alert code
        resultStream.foreach((key, value) -> {
            System.out.println("Result Stream: Key: " + key + ", Value: " + value);
        });

        resultStream.to("error-rate-alert-topic");

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
