package org.example.deduplicate;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.kstream.KStreamExample;

import java.io.FileInputStream;
import java.util.Properties;

public class DeDuplicationProducer {

    private static final Logger logger = LogManager.getLogger(DeDuplicationProducer.class);

    public static void main(String[] args) throws Exception {
        logger.info("Creating kafka producer......");
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream("src/main/resources/application.properties")) {
            props.load(in);
        }
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "Kafka-Producer");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("bootstrap.servers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        final String inputTopic = props.getProperty("input.topic.name");

        logger.info("Start sending messages...");
        for (int i = 1; i <= 5; i++) {
            producer.send(new ProducerRecord<>(inputTopic, String.valueOf(i),
                    "First Simple Message-" + i));
            producer.send(new ProducerRecord<>(inputTopic, String.valueOf(i),
                    "Second Simple Message-" + i));
        }

        logger.info("Finished - Closing Kafka Producer.");
        producer.close();
    }
}
