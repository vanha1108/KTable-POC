package org.example.demo.materializedviewmaintenance.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.example.demo.materializedviewmaintenance.UserBalance;

import java.io.IOException;
import java.util.Map;

public class UserBalanceSerde<T> implements Serde<UserBalance> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }


    @Override
    public Serializer<UserBalance> serializer() {
        objectMapper.registerModule(new JavaTimeModule());
        return (topic, data) -> {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing JSON", e);
            }
        };
    }

    @Override
    public Deserializer<UserBalance> deserializer() {
        objectMapper.registerModule(new JavaTimeModule());
        return (topic, data) -> {
            try {
                return objectMapper.readValue(data, UserBalance.class);
            } catch (IOException e) {
                throw new RuntimeException("Error deserializing JSON", e);
            }
        };
    }
}
