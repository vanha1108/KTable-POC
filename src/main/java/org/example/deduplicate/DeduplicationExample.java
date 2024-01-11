package org.example.deduplicate;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DeduplicationExample {

    private static final Logger logger = LoggerFactory.getLogger(DeduplicationExample.class);

    private static final String storeName = "eventId-store";

    public static void main(String[] args) throws IOException {
        final DeduplicationExample instance = new DeduplicationExample();
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream("src/main/resources/application.properties")) {
            props.load(in);
        }
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "deduplicate-example-6");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.String().getClass().getName());
        // Key a --> value 1
        // Key a --> value 2
        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = props.getProperty("input.topic.name");
        final String outTopic = props.getProperty("table.output.topic.name");

        KTable<String, String> firstKTable = builder.table(inputTopic,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("deduplicate-store-6")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()));

        // Deduplicate based on event ID
        final Duration windowSize = Duration.ofMinutes(1); // One minute to test
        final Duration retentionPeriod = windowSize;
        final StoreBuilder<WindowStore<String, Long>> dedupStoreBuilder = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(storeName,
                        retentionPeriod,
                        windowSize,
                        false
                ),
                Serdes.String(),
                Serdes.Long());
        builder.addStateStore(dedupStoreBuilder);
        KStream<String, String> deduplicatedStream = firstKTable.toStream().transform(() ->
                new DeduplicationTransformer<>(windowSize.toMillis(),
                        (key, value) -> value), storeName);

        // Print the deduplicated records
//        deduplicatedStream.foreach((key, value) -> {
//            System.out.println("Key: " + key + ", Value: " + value);
//            // Perform further processing or write to another topic if needed
//        });

        deduplicatedStream.to(outTopic);

        Topology topology = builder.build();

//        instance.createTopics(props);

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        logger.info("Starting stream.");
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down stream");
            kafkaStreams.close();
        }));
    }

    public static void createTopics(final Properties allProps) {
        try (final AdminClient client = AdminClient.create(allProps)) {

            final List<NewTopic> topics = new ArrayList<>();

            topics.add(new NewTopic(
                    allProps.getProperty("input.topic.name"),
                    Integer.parseInt(allProps.getProperty("input.topic.partitions")),
                    Short.parseShort(allProps.getProperty("input.topic.replication.factor"))));

            topics.add(new NewTopic(
                    allProps.getProperty("table.output.topic.name"),
                    Integer.parseInt(allProps.getProperty("table.output.topic.partitions")),
                    Short.parseShort(allProps.getProperty("table.output.topic.replication.factor"))));

            client.createTopics(topics);
        }
    }
}
