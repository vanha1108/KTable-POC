package org.example.demo.statefulstreamprocessing;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.example.demo.materializedviewmaintenance.serdes.JsonSerde;
import org.example.demo.statefulstreamprocessing.serdes.ProductSaleTotalSerde;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class StatefulStreamProccessingDemo {

    public static void main(String[] args) throws IOException {
        runPipelineStatefulStreamProcessing();
    }

    private static void runPipelineStatefulStreamProcessing() throws IOException {
        // Set up Kafka Streams properties
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream("src/main/resources/application.properties")) {
            props.load(in);
        }
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "state-stream-processing-test-3");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ProductSaleTotalSerde.class.getName());

        // Use JsonSerde for SaleEvent and ProductSaleTotal
        JsonSerde<SaleEvent> saleEventSerde = new JsonSerde<>(SaleEvent.class);
        JsonSerde<ProductSaleTotal> productSaleTotalSerde = new JsonSerde<>(ProductSaleTotal.class);

        // Create a StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();

        // Create a KStream from the "sale-event-topic"
        KStream<String, SaleEvent> saleEventStream = builder.stream(
                "sale-event-topic",
                Consumed.with(Serdes.String(), saleEventSerde)
        );

        // Aggregate total quantity sold for each product ID
        saleEventStream
                .groupBy((productId, saleEvent) -> productId, Grouped.with(Serdes.String(), saleEventSerde))
                .aggregate(
                        ProductSaleTotal::new,
                        (productId, saleEvent, productSaleTotal) -> {
                            productSaleTotal.updateTotalQuantity(saleEvent.getQuantity());
                            return productSaleTotal;
                        },
                        Materialized.as("product-sale-total")
                )
                .toStream()
                .peek(((key, value) -> System.out.println("Key: " + key + " value: " + value)))
                .to("product-sale-total-topic", Produced.with(Serdes.String(), productSaleTotalSerde));

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
