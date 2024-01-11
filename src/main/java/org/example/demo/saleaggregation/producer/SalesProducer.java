package org.example.demo.saleaggregation.producer;

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

public class SalesProducer {

    public static final String SALE_TOPIC = "sales";

    public static void main(String[] args) throws IOException {
        // Set up Kafka producer properties
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream("src/main/resources/application.properties")) {
            props.load(in);
        }
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("bootstrap.servers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 1; i <= 5; i++) {
            producer.send(new ProducerRecord<>(SALE_TOPIC,
                    String.valueOf(i),
                    String.valueOf(i * 100)));
            producer.send(new ProducerRecord<>(SALE_TOPIC,
                    String.valueOf(i),
                    String.valueOf(i * 200)));
        }

        // Close the producer
        producer.close();
        System.out.println("Completed to producer...");
    }
}
