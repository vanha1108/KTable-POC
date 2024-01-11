package org.example.demo.materializedviewmaintenance.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.example.demo.materializedviewmaintenance.TransactionEvent;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class TransactionProducer {

    public static final String TRANSACTION_TOPIC = "transaction-topic";

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

        for (int i = 1; i < 3; i++) {
            String key = String.valueOf(i);

            TransactionEvent transactionEvent = new TransactionEvent();
            transactionEvent.setUserId("1");
            transactionEvent.setAmount(i * 100);

            producer.send(new ProducerRecord<>(TRANSACTION_TOPIC, key, objectMapper.writeValueAsString(transactionEvent)));
        }

        // Close the producer
        producer.close();
        System.out.println("Completed to producer...");
    }
}
