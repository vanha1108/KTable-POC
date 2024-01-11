package org.example.demo.saleaggregation;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ToTalSalesAggregationDemo {

    public static final String TOPIC_NAME = "sales";

    public static void main(String[] args) throws IOException {
        runPipelineTotalSales();
    }

    private static void runPipelineTotalSales() throws IOException {
        // Set up Kafka Streams properties
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream("src/main/resources/application.properties")) {
            props.load(in);
        }
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "total-sales-test-12");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.Long().getClass().getName());

        // Create a StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> salesStream = builder.stream(TOPIC_NAME, Consumed.with(Serdes.String(), Serdes.String()));

        KTable<String, String> totalSalesTable = salesStream.groupByKey().aggregate(() -> 0L,
                ((key, value, aggregate) -> Long.sum(aggregate, Long.valueOf(value))))
                .mapValues(value -> String.valueOf(value));

        totalSalesTable.toStream().foreach((key, value) -> {
            System.out.println("Key: " + key + ", Value: " + value);
        });

        // Output the aggregated results to a new topic "product-sales-aggregated"
        totalSalesTable.toStream().to("total-sales-topic", Produced.with(Serdes.String(), Serdes.String()));

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
