package org.example.broker;

import static org.junit.jupiter.api.Assertions.*;

import org.example.broker.consumer.Consumer;
import org.example.broker.core.Message;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;

class ConsumerTest {

    private static final String TOPIC = "test-topic";
    private static final String CONSUMER_ID = "test-consumer";
    private static final String PRODUCER_ID = "test-consumer";


    @Test
    void givenAtLeastOnceDelivery_whenOnMessage_thenAcknowledgesAndUpdatesOffset() {
        Consumer<String> consumer =
                new Consumer<>(CONSUMER_ID, Collections.singleton(TOPIC), DeliveryOption.AT_LEAST_ONCE);
        Message<String> msg = new Message<>("test-message", TOPIC, 1000, 5, PRODUCER_ID);
        assertTrue(consumer.onMessage(msg));
        assertTrue(msg.isAcknowledged());
        assertEquals(5, consumer.getLastOffset(TOPIC));
    }

    @Test
    void givenExactlyOnceDeliveryAndDuplicate_whenOnMessage_thenDeduplicatesButStillAcks() {
        Consumer<String> consumer =
                new Consumer<>(CONSUMER_ID, Collections.singleton(TOPIC), DeliveryOption.EXACTLY_ONCE);
        Message<String> m1 = new Message<>("test-message", TOPIC, 1000, 1, PRODUCER_ID);
        assertTrue(consumer.onMessage(m1));

        Message<String> dup = new Message<>("test-message", TOPIC, 1000, 1, PRODUCER_ID) {
            @Override
            public UUID getId() {
                return m1.getId();
            }
        };
        assertTrue(consumer.onMessage(dup));
        assertTrue(dup.isAcknowledged());
        assertEquals(1, consumer.getLastOffset(TOPIC));
    }

    @Test
    void givenPayloadThrowsException_whenOnMessage_thenReturnsFalse() {
        Consumer<String> consumer =
                new Consumer<>(CONSUMER_ID, Collections.singleton(TOPIC), DeliveryOption.AT_LEAST_ONCE);
        Message<String> bad = new Message<>("test-message", TOPIC, 1000, 2, PRODUCER_ID) {
            @Override
            public String getPayload() {
                throw new RuntimeException("fail");
            }
        };
        assertFalse(consumer.onMessage(bad));
    }
}
