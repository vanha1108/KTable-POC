package org.example.demo.joinoperations;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.example.demo.materializedviewmaintenance.serdes.JsonSerde;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class JoinOperationsDemo {

    public static void main(String[] args) throws IOException {
        runPipelineJoinOperations();
    }

    private static void runPipelineJoinOperations() throws IOException {
        // Set up Kafka Streams properties
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream("src/main/resources/application.properties")) {
            props.load(in);
        }
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-operations-test-4");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Use JsonSerde for Order, ProductInfo, and EnrichedOrder
        JsonSerde<Order> orderSerde = new JsonSerde<>(Order.class);
        JsonSerde<ProductInfo> productInfoSerde = new JsonSerde<>(ProductInfo.class);
        JsonSerde<EnrichedOrder> enrichedOrderSerde = new JsonSerde<>(EnrichedOrder.class);

        // Create a StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();

        // Create KStreams from the "order-topic" and "product-info-topic"
        KStream<String, Order> orderStream = builder.stream(
                "order-topic",
                Consumed.with(Serdes.String(), orderSerde)
        );

        GlobalKTable<String, ProductInfo> productInfoTable = builder.globalTable(
                "product-info-topic",
                Consumed.with(Serdes.String(), productInfoSerde)
        );

        ValueJoiner<Order, ProductInfo, EnrichedOrder> valueJoiner = (order, productInfo) -> {
            if (productInfo != null) {
                double totalPrice = order.getQuantity() * productInfo.getPrice();
                return new EnrichedOrder(order.getOrderId(), productInfo.getProductName(), order.getQuantity(), totalPrice);
            } else {
                return null; // Handle cases where product info is not available
            }
        };


        // Join streams to enrich orders with product details
        orderStream.leftJoin(productInfoTable,
                        (key, value) -> value.getOrderId(),
                        valueJoiner)
                .filter((key, value) -> value != null)
                .peek((key, value) -> System.out.println("Key: " + key + " value: " + value))
                .to("enriched-order-topic", Produced.with(Serdes.String(), enrichedOrderSerde));


//        KStream<String, EnrichedOrder> enrichedOrderStream = orderStream
//                .selectKey((orderId, order) -> order.getProductId()) // Select product ID as the key
//                .leftJoin(
//                        productInfoTable,
//                        (order, productInfo) -> {
//                            if (productInfo != null) {
//                                double totalPrice = order.getQuantity() * productInfo.getPrice();
//                                return new EnrichedOrder(order.getOrderId(), productInfo.getProductName(), order.getQuantity(), totalPrice);
//                            } else {
//                                return null; // Handle cases where product info is not available
//                            }
//                        },
//                        Joined.with(Serdes.String(), orderSerde, productInfoSerde)
//                );

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
