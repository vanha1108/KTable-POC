package org.example.demo.materializedviewmaintenance.mapper;

import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.example.demo.materializedviewmaintenance.TransactionEvent;

public class UserIdMapper implements KeyValueMapper<String, TransactionEvent, String> {
    @Override
    public String apply(String key, TransactionEvent value) {
        if (value != null) {
            return value.getUserId();
        }
        return "";
    }
}
