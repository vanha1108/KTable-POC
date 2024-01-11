package org.example.demo.windowedaggregation.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.example.demo.windowedaggregation.TemperatureReading;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Properties;

public class TemperatureProducer {

    public static final String TEMPERATURE_TOPIC = "temperature-topic-2";

    public static void main(String[] args) throws IOException, InterruptedException {
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
            TemperatureReading reading = new TemperatureReading();
            reading.setSensorId(String.valueOf(i));
            reading.setTemperature(35);
            reading.setTimestamp(Instant.now());
            producer.send(new ProducerRecord<>(TEMPERATURE_TOPIC, String.valueOf(i), objectMapper.writeValueAsString(reading)));
        }
        Thread.sleep(30000);
        TemperatureReading reading = new TemperatureReading();
        reading.setSensorId(String.valueOf(1));
        reading.setTemperature(45);
        reading.setTimestamp(Instant.now());
        producer.send(new ProducerRecord<>(TEMPERATURE_TOPIC, String.valueOf(1), objectMapper.writeValueAsString(reading)));
        Thread.sleep(60000);
        TemperatureReading reading2 = new TemperatureReading();
        reading2.setSensorId(String.valueOf(2));
        reading2.setTemperature(45);
        reading2.setTimestamp(Instant.now());
        producer.send(new ProducerRecord<>(TEMPERATURE_TOPIC, String.valueOf(2), objectMapper.writeValueAsString(reading2)));

        // Close the producer
        producer.close();
        System.out.println("Completed to producer...");
    }
}
