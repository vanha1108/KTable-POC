package org.example.demo.dataenrichment;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.example.demo.materializedviewmaintenance.serdes.JsonSerde;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

public class DataEnrichmentDemo {

    public static void main(String[] args) throws IOException {
        runPipelineDataEnrichment();
    }

    private static void runPipelineDataEnrichment() throws IOException {
        // Set up Kafka Streams properties
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream("src/main/resources/application.properties")) {
            props.load(in);
        }
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "data-enrichment-test-27");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());


        // Use JsonSerde for UserActivityEvent, UserData, and EnrichedUserActivityEvent
        JsonSerde<UserActivityEvent> userActivityEventSerde = new JsonSerde<>(UserActivityEvent.class);
        JsonSerde<UserData> userDataSerde = new JsonSerde<>(UserData.class);
        JsonSerde<EnrichedUserActivityEvent> enrichedUserActivityEventSerde = new JsonSerde<>(EnrichedUserActivityEvent.class);

        // Create a StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();

        // Create KStreams from the "user-activity-topic" and KTable from the "user-data-topic"
        KStream<String, UserActivityEvent> userActivityEventStream = builder.stream(
                "user-activity-topic",
                Consumed.with(Serdes.String(), userActivityEventSerde)
        );

        KStream<String, UserData> userDataStream = builder.stream(
                "user-data-topic",
                Consumed.with(Serdes.String(), userDataSerde)
        );

        ValueJoiner<UserActivityEvent, UserData, EnrichedUserActivityEvent> valueJoiner = (activity, userData) -> {
            if (userData != null) {
                return new EnrichedUserActivityEvent(
                        activity.getUserId(),
                        userData.getUserName(),
                        activity.getActivity(),
                        userData.getLocation(),
                        activity.getTimestamp()
                );
            } else {
                return null; // Handle cases where user data is not available
            }
        };

        StreamJoined<String, UserActivityEvent, UserData> streamJoined =
                StreamJoined.with(Serdes.String(), userActivityEventSerde, userDataSerde);

        KStream<String, EnrichedUserActivityEvent> enrichedUserActivityEventStream = userActivityEventStream
                .leftJoin(
                        userDataStream,
                        valueJoiner,
                        JoinWindows.of(Duration.ofMinutes(1)),
                        streamJoined)
                .filter((key, value) -> value != null);

        enrichedUserActivityEventStream.foreach((key, value) -> {
            System.out.println("Enrichment stream: Key: " + key + ", Value: " + value);
        });

        enrichedUserActivityEventStream.toTable();


        // Output enriched user activity events to "enriched-user-activity-topic"
        enrichedUserActivityEventStream.to("enriched-user-activity-topic", Produced.with(Serdes.String(), enrichedUserActivityEventSerde));


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
