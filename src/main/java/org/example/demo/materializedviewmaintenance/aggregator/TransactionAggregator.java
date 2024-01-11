package org.example.demo.materializedviewmaintenance.aggregator;

import org.apache.kafka.streams.kstream.Aggregator;
import org.example.demo.materializedviewmaintenance.TransactionEvent;
import org.example.demo.materializedviewmaintenance.UserBalance;

public class TransactionAggregator implements Aggregator<String, TransactionEvent, UserBalance> {

    @Override
    public UserBalance apply(String key, TransactionEvent value, UserBalance aggregate) {
        aggregate.updateBalance(value.getAmount());
        return aggregate;
    }
}
