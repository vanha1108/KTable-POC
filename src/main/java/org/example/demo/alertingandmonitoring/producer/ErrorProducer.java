package org.example.demo.alertingandmonitoring.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.example.demo.alertingandmonitoring.ErrorEvent;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Properties;

public class ErrorProducer {

    public static final String ERROR_TOPIC = "error-event-topic";

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

        ErrorEvent event1 = new ErrorEvent();
        event1.setEventId("1");
        event1.setErrorMessage("This is message 1" + 1);
        event1.setTimestamp(Instant.now());

        producer.send(new ProducerRecord<>(ERROR_TOPIC, event1.getEventId(), objectMapper.writeValueAsString(event1)));

//        for (int i = 1; i < 10; i++) {
//            ErrorEvent event1 = new ErrorEvent();
//            event1.setEventId("1");
//            event1.setErrorMessage("This is message 1" + i);
//            event1.setTimestamp(Instant.now());
//
//            ErrorEvent event2 = new ErrorEvent();
//            event2.setEventId("2");
//            event2.setErrorMessage("This is message 2" + i);
//            event2.setTimestamp(Instant.now());
//
//            producer.send(new ProducerRecord<>(ERROR_TOPIC, event1.getEventId(), objectMapper.writeValueAsString(event1)));
//            producer.send(new ProducerRecord<>(ERROR_TOPIC, event2.getEventId(), objectMapper.writeValueAsString(event2)));
//        }

        // Close the producer
        producer.close();
        System.out.println("Completed to producer...");
    }
}
