package org.example.ktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Properties;

public class KTableExample {

    private static final Logger logger = LoggerFactory.getLogger(KTableExample.class);

    public static void main(String[] args) throws Exception {
        Properties streamProperties = new Properties();
        try (FileInputStream in = new FileInputStream("src/main/resources/application.properties")) {
            streamProperties.load(in);
        }
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-example-2");
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.String().getClass().getName());
        streamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = streamProperties.getProperty("input.topic.name");
        final String outTopic = streamProperties.getProperty("table.output.topic.name");

        KTable<String, String> firstKTable = builder.table(inputTopic,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("ktable-store-2")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()));

//        firstKTable.mapValues(value -> value = value.concat("123"))
//                .toStream()
//                .peek(((key, value) -> System.out.println("Outgoing record - key:" + key + " value: " + value)))
//                .to(outTopic, Produced.with(Serdes.String(), Serdes.String()));

        // Deduplicate based on event ID
        KTable<String, String> deduplicatedTable = firstKTable
                .groupBy((key, value) -> KeyValue.pair(key, value))
                .reduce(
                        // Reducer
                        (oldValue, newValue) -> newValue,
                        (oldValue, newValue) -> newValue
                );

        // Print the deduplicated records
        deduplicatedTable.toStream().foreach((key, value) -> {
            System.out.println("Key: " + key + ", Value: " + value);
            // Perform further processing or write to another topic if needed
        });

        Topology topology = builder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamProperties);
        logger.info("Starting stream.");
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down stream");
            kafkaStreams.close();
        }));
    }
}
