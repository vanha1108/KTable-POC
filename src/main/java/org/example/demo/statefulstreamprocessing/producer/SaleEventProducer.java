package org.example.demo.statefulstreamprocessing.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.example.demo.statefulstreamprocessing.SaleEvent;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Properties;

public class SaleEventProducer {

    public static final String SALE_EVENT_TOPIC = "sale-event-topic";

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

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

//        SaleEvent saleEvent = new SaleEvent();
//        saleEvent.setProductId(String.valueOf(1));
//        saleEvent.setQuantity(2 );
//        saleEvent.setTimestamp(Instant.now());
//
//        producer.send(new ProducerRecord<>(SALE_EVENT_TOPIC,
//                saleEvent.getProductId(),
//                objectMapper.writeValueAsString(saleEvent)
//        ));

        for (int i = 1; i < 5; i++) {
            SaleEvent saleEvent = new SaleEvent();
            saleEvent.setProductId(String.valueOf(i));
            saleEvent.setQuantity(2 * i);
            saleEvent.setTimestamp(Instant.now());

            producer.send(new ProducerRecord<>(SALE_EVENT_TOPIC,
                    saleEvent.getProductId(),
                    objectMapper.writeValueAsString(saleEvent)
            ));
        }

        // Close the producer
        producer.close();
        System.out.println("Completed to producer...");
    }
}
