package org.example.demo.joinoperations.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.example.demo.joinoperations.Order;
import org.example.demo.joinoperations.ProductInfo;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class OrderProducer {

    public static final String ORDER_TOPIC = "order-topic";

    public static final String PRODUCT_INFO_TOPIC = "product-info-topic";

    public static void main(String[] args) throws IOException {
        // Set up Kafka producer properties
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream("src/main/resources/application.properties")) {
            props.load(in);
        }
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("bootstrap.servers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        // Create Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 1; i < 5; i++) {
            String key = String.valueOf(i);

            Order order = new Order();
            order.setOrderId(key);
            order.setProductId(key);
            order.setQuantity(2 * i);

            ProductInfo productInfo = new ProductInfo();
            productInfo.setProductId(key);
            productInfo.setProductName("Product " + key);
            productInfo.setPrice(300 * i);

            producer.send(new ProducerRecord<>(ORDER_TOPIC, key, objectMapper.writeValueAsString(order)));
            producer.send(new ProducerRecord<>(PRODUCT_INFO_TOPIC, key, objectMapper.writeValueAsString(productInfo)));
        }

        // Close the producer
        producer.close();
        System.out.println("Completed to producer...");
    }
}
