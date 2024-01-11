package org.example.deduplicate;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeduplicationTransformer<K, V, E> implements Transformer<K, V, KeyValue<K, V>> {

    private static final Logger logger = LoggerFactory.getLogger(DeduplicationTransformer.class);

    private static final String storeName = "eventId-store";

    private ProcessorContext context;

    private WindowStore<E, Long> eventIdStore;

    private final KeyValueMapper<K, V, E> idExtractor;

    private final long leftDurationMs;

    private final long rightDurationMs;

    DeduplicationTransformer(final long maintainDurationPerEventInMs, final KeyValueMapper<K, V, E> idExtractor) {
        logger.info("Starting transform with maintainDurationPerEventInMs {}", maintainDurationPerEventInMs);
        if (maintainDurationPerEventInMs < 1) {
            throw new IllegalArgumentException("maintain duration per event must be >= 1");
        }
        leftDurationMs = maintainDurationPerEventInMs / 2;
        rightDurationMs = maintainDurationPerEventInMs - leftDurationMs;
        this.idExtractor = idExtractor;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        eventIdStore = context.getStateStore(storeName);
    }

    @Override
    public KeyValue<K, V> transform(K key, V value) {
        final E eventId = idExtractor.apply(key, value);
        logger.info("Preparing to compare with event id: {}", eventId.toString());
        if (eventId == null) {
            return KeyValue.pair(key, value);
        } else {
            final KeyValue<K, V> output;
            if (isDuplicate(eventId)) {
                output = null;
            } else {
                output = KeyValue.pair(key, value);
            }
            updateTimestampOrRememberNewEvent(eventId, context.timestamp());
            return output;
        }
    }

    @Override
    public void close() {
        // Note: The store should NOT be closed manually here via `eventIdStore.close()`!
        // The Kafka Streams API will automatically close stores when necessary.
    }

    private boolean isDuplicate(final E eventId) {
        final long eventTime = context.timestamp();
        final WindowStoreIterator<Long> timeIterator = eventIdStore.fetch(
                eventId,
                eventTime - leftDurationMs,
                eventTime + rightDurationMs);
        final boolean isDuplicate = timeIterator.hasNext();
        timeIterator.close();
        return isDuplicate;
    }

    private void updateTimestampOrRememberNewEvent(final E eventId, final long newTimestamp) {
        eventIdStore.put(eventId, newTimestamp, newTimestamp);
    }
}
