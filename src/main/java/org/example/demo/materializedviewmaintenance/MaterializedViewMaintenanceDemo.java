package org.example.demo.materializedviewmaintenance;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.example.demo.materializedviewmaintenance.aggregator.TransactionAggregator;
import org.example.demo.materializedviewmaintenance.mapper.UserIdMapper;
import org.example.demo.materializedviewmaintenance.serdes.JsonSerde;
import org.example.demo.materializedviewmaintenance.serdes.UserBalanceSerde;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class MaterializedViewMaintenanceDemo {

    public static void main(String[] args) throws IOException {
        runPipelineMaterializedViewMaintenance();
    }

    private static void runPipelineMaterializedViewMaintenance() throws IOException {
        // Set up Kafka Streams properties
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream("src/main/resources/application.properties")) {
            props.load(in);
        }
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "materialized-view-maintenance-test-23");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, UserBalanceSerde.class.getName());

        // Create a StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();

        // Create a KStream from the "transaction-topic"
        KStream<String, TransactionEvent> transactionStream = builder.stream(
                "transaction-topic",
                Consumed.with(Serdes.String(), new JsonSerde<>(TransactionEvent.class))
        );

        UserIdMapper userIdMapper = new UserIdMapper();
        TransactionAggregator transactionAggregator = new TransactionAggregator();


        // Aggregate total balance for each user
        KTable<String, UserBalance> resultTable = transactionStream.groupBy(userIdMapper::apply)
                .aggregate(UserBalance::new,
                        transactionAggregator::apply
                );

        resultTable.toStream().peek(((key, value) -> System.out.println("Key: " + key + " value: " + value)));

        resultTable.toStream().to("user-balance-topic",
                Produced.with(Serdes.String(), new JsonSerde<>(UserBalance.class)));

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
