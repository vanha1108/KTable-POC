package org.example.demo.statefulstreamprocessing.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.example.demo.statefulstreamprocessing.ProductSaleTotal;

import java.io.IOException;
import java.util.Map;

public class ProductSaleTotalSerde<T> implements Serde<ProductSaleTotal> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }


    @Override
    public Serializer<ProductSaleTotal> serializer() {
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
    public Deserializer<ProductSaleTotal> deserializer() {
        objectMapper.registerModule(new JavaTimeModule());
        return (topic, data) -> {
            try {
                return objectMapper.readValue(data, ProductSaleTotal.class);
            } catch (IOException e) {
                throw new RuntimeException("Error deserializing JSON", e);
            }
        };
    }
}
