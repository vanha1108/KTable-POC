package org.example.demo.dataenrichment.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.example.demo.dataenrichment.UserActivityEvent;
import org.example.demo.dataenrichment.UserData;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Properties;

public class UserEnrichmentProducer {

    public static final String USER_ACTIVITY_TOPIC = "user-activity-topic";
    public static final String USER_DATA_TOPIC = "user-data-topic";

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

            UserActivityEvent userEvent = new UserActivityEvent();
            userEvent.setUserId(String.valueOf(i));
            userEvent.setActivity("BUY PHONE" + i);
            userEvent.setTimestamp(Instant.now());

            UserData userData = new UserData();
            userData.setUserId(String.valueOf(i));
            userData.setLocation("HCM");
            userData.setUserName("user1");

            producer.send(new ProducerRecord<>(USER_ACTIVITY_TOPIC, key, objectMapper.writeValueAsString(userEvent)));
            producer.send(new ProducerRecord<>(USER_DATA_TOPIC, key, objectMapper.writeValueAsString(userData)));
        }

        // Close the producer
        producer.close();
        System.out.println("Completed to producer...");
    }
}
