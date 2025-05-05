package org.example.broker;

import static org.junit.jupiter.api.Assertions.*;

import org.example.broker.core.Message;
import org.example.broker.core.MessageBroker;
import org.example.broker.producer.IProducer;
import org.example.broker.producer.Producer;
import org.junit.jupiter.api.*;

class ProducerTest {

    @Test
    void givenNullBroker_whenConstructingProducer_thenThrowsIllegalArgument() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new Producer<String>(null, 1000, DeliveryOption.BROADCAST));
        assertEquals("MessageBroker cannot be null", exception.getMessage());

    }

    @Test
    void givenNonPositiveTTL_whenConstructingProducer_thenThrowsIllegalArgument() {
        MessageBroker<String> broker = new MessageBroker<>();
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new Producer<>(broker, 0, DeliveryOption.BROADCAST));
        assertEquals("Default TTL must be positive", exception.getMessage());
    }

    @Test
    void givenValidBrokerAndTTL_whenSend_thenReturnsMessageWithSameProducerIdAndPayload() {
        MessageBroker<String> broker = new MessageBroker<>();
        IProducer<String> producer = new Producer<>(broker, 1000, DeliveryOption.BROADCAST);
        Message<String> m = producer.send("test-message", "topic");

        assertNotNull(m);
        assertEquals(producer.getProducerId(), m.getProducerId());
        assertEquals("test-message", m.getPayload());
    }
}
