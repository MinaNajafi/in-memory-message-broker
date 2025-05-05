package org.example.broker;

import static org.junit.jupiter.api.Assertions.*;

import org.example.broker.consumer.Consumer;
import org.example.broker.core.Message;
import org.example.broker.core.MessageBroker;
import org.junit.jupiter.api.*;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Collections;


class MessageBrokerTest {

    private MessageBroker<String> broker;
    private static final String TOPIC = "test-topic";
    private static final String CONSUMER_ID = "test-consumer";
    private static final String PRODUCER_ID = "test-producer";

    @BeforeEach
    void givenDefaultBroker() {
        broker = new MessageBroker<>(10, 2, true);
        broker.createTopic(TOPIC);
    }

    @Test
    void givenNullPayload_whenPublish_thenThrowsIllegalArgument() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> broker.publish(null, TOPIC, DeliveryOption.BROADCAST, 1000, PRODUCER_ID));
        assertEquals("Payload cannot be null", exception.getMessage());
    }

    @Test
    void givenNullTopic_whenPublish_thenThrowsIllegalArgument() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> broker.publish(CONSUMER_ID, null, DeliveryOption.BROADCAST, 1000, PRODUCER_ID));
        assertEquals("Topic cannot be null or empty", exception.getMessage());
    }

    @Test
    void givenNullConsumer_whenRegister_thenThrowsIllegalArgument() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> broker.registerConsumer(null));
        assertEquals("Consumer cannot be null", exception.getMessage());
    }

    @Test
    void givenNullConsumer_whenUnregister_thenThrowsIllegalArgument() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> broker.unregisterConsumer(null));
        assertEquals("Consumer cannot be null", exception.getMessage());
    }

    @Test
    void givenNoGuaranteesAndFullQueue_whenPublish_thenSwallowsBackpressureAndOffsetUnchanged() {
        MessageBroker<String> messageBroker =
                new MessageBroker<>(1, 1, true);
        messageBroker.publish("message-offset-1", TOPIC, DeliveryOption.BROADCAST, 1000, PRODUCER_ID);
        messageBroker.publish("message-offset-2", TOPIC, DeliveryOption.BROADCAST, 1000, PRODUCER_ID);
        assertEquals(0, messageBroker.getCurrentOffset(TOPIC));
    }

    @Test
    void givenRegisteredConsumer_whenPublish_thenDeliversAsynchronously() throws InterruptedException {
        AtomicBoolean delivered = new AtomicBoolean(false);
        Consumer<String> consumer = new Consumer<>("consumer-async-test", Collections.singleton(TOPIC), DeliveryOption.AT_LEAST_ONCE) {
            @Override
            public boolean onMessage(Message<String> message) {
                delivered.set(true);
                System.out.println("Received message: " + message.getPayload());
                return super.onMessage(message);
            }
        };

        broker.registerConsumer(consumer);
        broker.publish("test-message", TOPIC, DeliveryOption.BROADCAST, 1000, PRODUCER_ID);
        Thread.sleep(100);

        assertTrue(delivered.get());
    }

    @Test
    void givenConsumerFailsInitially_whenPublish_thenRetriesUpToMaxAttempts() throws InterruptedException {
        AtomicBoolean succeeded = new AtomicBoolean(false);
        Consumer<String> consumer = new Consumer<>(CONSUMER_ID, Collections.singleton(TOPIC), DeliveryOption.AT_LEAST_ONCE) {
            @Override
            public boolean onMessage(Message<String> message) {
                if (message.getAttemptCount() >= 2) {
                    succeeded.set(true);
                    return super.onMessage(message);
                }
                return false;
            }
        };

        broker.registerConsumer(consumer);
        broker.publish("test-message", TOPIC, DeliveryOption.BROADCAST, 1000, PRODUCER_ID);
        Thread.sleep(3000);

        assertTrue(succeeded.get());
    }

    @Test
    void given_validTopic_when_publishMessage_then_messageStored() {

        String data = "test-message";

        Message<String> message = broker.publish(data, TOPIC, DeliveryOption.BROADCAST, 1000, PRODUCER_ID);

        assertNotNull(message);
        assertEquals(data, message.getPayload());
        assertEquals(TOPIC, message.getTopic());
        assertEquals(0, message.getOffset());
        assertTrue(message.getTimestamp() > 0);
        assertEquals(PRODUCER_ID, message.getProducerId());
    }

    @Test
    void given_nonExistentTopic_when_publishMessage_then_topicCreated() {

        String newTopic = "new-topic";
        String data = "test message for new topic";

        Message<String> message = broker.publish(data, newTopic, DeliveryOption.BROADCAST, 1000, PRODUCER_ID);

        assertNotNull(message);
        assertEquals(data, message.getPayload());
        assertEquals(newTopic, message.getTopic());
        assertEquals(0, message.getOffset());
    }


    @Test
    void given_expiredMessages_when_cleanup_then_messagesRemoved() throws InterruptedException {
        broker = new MessageBroker<>(10, 1, true);
        broker.createTopic(TOPIC);
        broker.publish("Expiring message", TOPIC, DeliveryOption.BROADCAST, 100, PRODUCER_ID);
        Thread.sleep(200);
        assertEquals(0, broker.getCurrentOffset(TOPIC));
    }


    @AfterEach
    void tearDown() {
        broker.disableCleanUp();
    }

}
