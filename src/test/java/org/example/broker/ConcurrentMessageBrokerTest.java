package org.example.broker;

import org.example.broker.consumer.IConsumer;
import org.example.broker.consumer.Consumer;
import org.example.broker.core.Message;
import org.example.broker.core.MessageBroker;
import org.example.broker.producer.IProducer;
import org.example.broker.producer.Producer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class ConcurrentMessageBrokerTest {

    private MessageBroker<String> messageBroker;
    private final int BACKPRESSURE_THRESHOLD = 5000;
    private final int MAX_RETRY_ATTEMPTS = 3;
    private static final String TOPIC = "test-topic";
    private static final String CONSUMER_ID = "test-consumer";
    private static final String PRODUCER_ID = "test-producer";

    @BeforeEach
    void setUp() {
        messageBroker = new MessageBroker<>(BACKPRESSURE_THRESHOLD, MAX_RETRY_ATTEMPTS, false);
    }

    @AfterEach
    void tearDown() {
        messageBroker.disableCleanUp();
    }

    @Test
    void given_multipleProducers_when_publishConcurrently_then_allMessagesDelivered() throws InterruptedException {

        final String TOPIC = "concurrent-publish-topic";
        int producerCount = 5;
        int messagesPerProducer = 100;

        List<IProducer<String>> producers = IntStream.range(0, producerCount)
                .mapToObj(_ ->(IProducer<String>) new Producer<>(messageBroker, 60000, DeliveryOption.AT_LEAST_ONCE))
                .toList();

        final ConcurrentHashMap<String, AtomicInteger> receivedMessages = new ConcurrentHashMap<>();
        final CountDownLatch messagesReceived = new CountDownLatch(producerCount * messagesPerProducer);

        IConsumer<String> testConsumer = new Consumer<>(
                "test-consumer",
                Collections.singleton(TOPIC),
                DeliveryOption.AT_LEAST_ONCE
        ) {
            @Override
            public boolean onMessage(Message<String> message) {
                String payload = message.getPayload();
                receivedMessages.computeIfAbsent(payload, k -> new AtomicInteger(0)).incrementAndGet();
                messagesReceived.countDown();
                return super.onMessage(message);
            }
        };

        messageBroker.registerConsumer(testConsumer);

        boolean allMessagesReceived;
        try (ExecutorService executorService = Executors.newFixedThreadPool(producerCount)) {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch producersComplete = new CountDownLatch(producerCount);

            for (int i = 0; i < producerCount; i++) {
                final int producerId = i;
                final IProducer<String> producer = producers.get(i);

                executorService.submit(() -> {
                    try {
                        startLatch.await();
                        for (int j = 0; j < messagesPerProducer; j++) {
                            String payload = "producer-" + producerId + "-message-" + j;
                            producer.send(payload, TOPIC);
                        }
                    } catch (Exception e) {
                        System.err.println("Error registering producer: " + e.getMessage());
                    } finally {
                        producersComplete.countDown();
                    }
                });
            }

            startLatch.countDown();
            producersComplete.await();

            allMessagesReceived = messagesReceived.await(5, TimeUnit.SECONDS);
            executorService.shutdown();
            executorService.awaitTermination(2, TimeUnit.SECONDS);
        }

        assertTrue(allMessagesReceived, "All messages should be received by the consumer");
        assertEquals(producerCount * messagesPerProducer, receivedMessages.size(),
                "All unique messages should be received");

        for (AtomicInteger count : receivedMessages.values()) {
            assertEquals(1, count.get(), "Each message should be received exactly once");
        }
    }

    @Test
    void given_multipleConsumers_when_registerConcurrently_then_allReceiveMessages() throws InterruptedException {

        final String TOPIC = "multi-consumer-topic";
        int consumerCount = 10;
        int messageCount = 50;

        List<TestIConsumer> consumers = new ArrayList<>();
        for (int i = 0; i < consumerCount; i++) {
            consumers.add(new TestIConsumer("consumer-" + i, Collections.singleton(TOPIC), DeliveryOption.BROADCAST));
        }

        try (ExecutorService executorService = Executors.newFixedThreadPool(consumerCount)) {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch consumersRegistered = new CountDownLatch(consumerCount);

            for (int i = 0; i < consumerCount; i++) {
                final TestIConsumer consumer = consumers.get(i);
                executorService.submit(() -> {
                    try {
                        startLatch.await();
                        messageBroker.registerConsumer(consumer);
                    } catch (Exception e) {
                        System.err.println("Error registering consumer: " + e.getMessage());
                    } finally {
                        consumersRegistered.countDown();
                    }
                });
            }

            startLatch.countDown();
            consumersRegistered.await();

            IProducer<String> producer = new Producer<>(messageBroker, 60000, DeliveryOption.BROADCAST);
            for (int i = 0; i < messageCount; i++) {
                producer.send("message-" + i, TOPIC);
            }

            Thread.sleep(1000);
            executorService.shutdown();
            executorService.awaitTermination(2, TimeUnit.SECONDS);
        }

        for (TestIConsumer consumer : consumers) {
            assertEquals(messageCount, consumer.getReceivedMessages().size(),
                    "Each consumer should receive all messages");

            for (int i = 0; i < messageCount; i++) {
                assertTrue(consumer.getReceivedMessages().contains("message-" + i),
                        "Consumer should receive message-" + i);
            }
        }
    }

    @Test
    void given_dynamicConsumers_when_registerAndUnregisterConcurrently_then_noMessageLoss() throws InterruptedException {

        final String TOPIC = "dynamic-consumers-topic";
        int totalConsumers = 20;
        int constantConsumers = 10;
        int messageCount = 200;

        List<TestIConsumer> permanentConsumers = new ArrayList<>();
        for (int i = 0; i < constantConsumers; i++) {
            TestIConsumer consumer = new TestIConsumer("permanent-" + i, Collections.singleton(TOPIC), DeliveryOption.AT_LEAST_ONCE);
            permanentConsumers.add(consumer);
            messageBroker.registerConsumer(consumer);
        }

        List<TestIConsumer> tempConsumers = new ArrayList<>();
        for (int i = 0; i < (totalConsumers - constantConsumers); i++) {
            tempConsumers.add(new TestIConsumer("temp-" + i, Collections.singleton(TOPIC), DeliveryOption.AT_LEAST_ONCE));
        }

        ExecutorService consumerExecutor;
        try (ExecutorService producerExecutor = Executors.newSingleThreadExecutor()) {
            AtomicInteger messagesSent = new AtomicInteger(0);
            CountDownLatch producerStarted = new CountDownLatch(1);
            CountDownLatch producerFinished = new CountDownLatch(1);

            producerExecutor.submit(() -> {
                try {
                    producerStarted.countDown();
                    IProducer<String> producer = new Producer<>(messageBroker, 60000, DeliveryOption.AT_LEAST_ONCE);
                    for (int i = 0; i < messageCount; i++) {
                        producer.send("continuous-message-" + i, TOPIC);
                        messagesSent.incrementAndGet();
                        Thread.sleep(10);
                    }
                } catch (Exception e) {
                    System.err.println("Error registering producer: " + e.getMessage());
                } finally {
                    producerFinished.countDown();
                }
            });

            producerStarted.await();

            consumerExecutor = Executors.newFixedThreadPool(tempConsumers.size());
            CountDownLatch consumerOpsComplete = new CountDownLatch(tempConsumers.size());

            for (int i = 0; i < tempConsumers.size(); i++) {
                final TestIConsumer consumer = tempConsumers.get(i);
                consumerExecutor.submit(() -> {
                    try {
                        messageBroker.registerConsumer(consumer);
                        Thread.sleep(ThreadLocalRandom.current().nextInt(200, 500));
                        messageBroker.unregisterConsumer(consumer);
                    } catch (Exception e) {
                        System.err.println("Error registering consumer: " + e.getMessage());
                    } finally {
                        consumerOpsComplete.countDown();
                    }
                });
            }

            consumerOpsComplete.await();
            producerFinished.await();

            Thread.sleep(1000);

            producerExecutor.shutdown();
            consumerExecutor.shutdown();
            producerExecutor.awaitTermination(2, TimeUnit.SECONDS);
        }
        consumerExecutor.awaitTermination(2, TimeUnit.SECONDS);

        Set<String> allReceivedMessages = new HashSet<>();
        for (TestIConsumer consumer : permanentConsumers) {
            allReceivedMessages.addAll(consumer.getReceivedMessages());
        }
        for (TestIConsumer consumer : tempConsumers) {
            allReceivedMessages.addAll(consumer.getReceivedMessages());
        }

        assertEquals(messageCount, allReceivedMessages.size(),
                "Every message should be received by at least one consumer");
        for (int i = 0; i < messageCount; i++) {
            assertTrue(allReceivedMessages.contains("continuous-message-" + i),
                    "Message continuous-message-" + i + " should be received by at least one consumer");
        }
    }

    @Test
    void given_concurrentCleanup_when_operatingWithExpiredMessages_then_maintainsConsistency() throws InterruptedException {

        final String TOPIC = "cleanup-test-topic";
        MessageBroker<String> brokerWithCleanup = new MessageBroker<>(
                BACKPRESSURE_THRESHOLD, MAX_RETRY_ATTEMPTS, true);

        TestIConsumer consumer = new TestIConsumer("cleanup-consumer", Collections.singleton(TOPIC), DeliveryOption.BROADCAST);
        brokerWithCleanup.registerConsumer(consumer);

        IProducer<String> producer = new Producer<>(brokerWithCleanup, 50, DeliveryOption.BROADCAST);

        AtomicInteger inconsistencies;
        try (ExecutorService executorService = Executors.newFixedThreadPool(2)) {
            CountDownLatch testComplete = new CountDownLatch(2);

            executorService.submit(() -> {
                try {
                    for (int i = 0; i < 500; i++) {
                        producer.send("short-lived-" + i, TOPIC);
                        if (i % 50 == 0) {
                            Thread.sleep(100);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error registering producer: " + e.getMessage());
                } finally {
                    testComplete.countDown();
                }
            });

            inconsistencies = new AtomicInteger(0);
            executorService.submit(() -> {
                try {
                    for (int i = 0; i < 10; i++) {
                        Thread.sleep(200);

                        long currentOffset = brokerWithCleanup.getCurrentOffset(TOPIC);

                        if (currentOffset < 0 && !consumer.getReceivedMessages().isEmpty()) {
                            inconsistencies.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error registering consumer: " + e.getMessage());
                } finally {
                    testComplete.countDown();
                }
            });

            testComplete.await();
            executorService.shutdown();
            executorService.awaitTermination(2, TimeUnit.SECONDS);
        }


        assertEquals(0, inconsistencies.get(), "No inconsistencies should be detected during cleanup operations");
    }


    @Test
    public void givenConcurrentProducers_whenAllStartTogetherForExactlyOnceMessage_thenAlwaysOneMessageIsConsumed() {


        MessageBroker<String> broker = new MessageBroker<>();
        Consumer<String> consumer1 = new Consumer<>(CONSUMER_ID, Collections.singleton(TOPIC), DeliveryOption.EXACTLY_ONCE) {
            @Override
            public boolean onMessage(Message<String> message) {
                return super.onMessage(message);
            }
        };

        Consumer<String> consumer2 = new Consumer<>("c3", Collections.singleton(TOPIC), DeliveryOption.EXACTLY_ONCE) {
            @Override
            public boolean onMessage(Message<String> message) {
                return super.onMessage(message);
            }
        };

        Consumer<String> consumer3 = new Consumer<>("c2", Collections.singleton(TOPIC), DeliveryOption.EXACTLY_ONCE) {
            @Override
            public boolean onMessage(Message<String> message) {
                return super.onMessage(message);
            }
        };

        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch endGate = new CountDownLatch(1000);

        try (ExecutorService exec = Executors.newFixedThreadPool(1000)) {

            for (int i = 0; i < 1000; i++) {
                int finalI = i;
                exec.submit(() -> {
                    try {
                        startGate.await();
                        if (finalI % 4 == 1) {
                            broker.registerConsumer(consumer1);
                        } else if (finalI % 4 == 2) {
                            broker.registerConsumer(consumer2);

                        } else {
                            broker.registerConsumer(consumer3);

                        }


                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } finally {
                        endGate.countDown();
                    }
                });
            }

            startGate.countDown();

            try {
                endGate.await();
            } catch (InterruptedException e) {
                System.err.println("Oops something bad happend during awaiting endGate");
            }
            exec.shutdown();
        }

        assertEquals(3, broker.getRegisteredConsumersCount(TOPIC));

    }

    @Test
    void givenMultipleConsumers_whenMessagePublished_thenAllConsumersReceiveIt() {
        MessageBroker<String> broker = new MessageBroker<>();
        String topic = TOPIC;
        broker.createTopic(topic);

        List<String> received1 = new CopyOnWriteArrayList<>();
        List<String> received2 = new CopyOnWriteArrayList<>();
        List<String> received3 = new CopyOnWriteArrayList<>();
        List<String> received4 = new CopyOnWriteArrayList<>();

        Consumer<String> consumer1 = new Consumer<>(CONSUMER_ID, Collections.singleton(TOPIC), DeliveryOption.EXACTLY_ONCE) {
            @Override
            public boolean onMessage(Message<String> message) {
                received1.add(message.getPayload());
                return super.onMessage(message);
            }
        };

        Consumer<String> consumer2 = new Consumer<>("test-consumer2", Collections.singleton(TOPIC), DeliveryOption.EXACTLY_ONCE) {
            @Override
            public boolean onMessage(Message<String> message) {
                received2.add(message.getPayload());
                return super.onMessage(message);
            }
        };

        Consumer<String> consumer3 = new Consumer<>("test-consumer3", Collections.singleton(TOPIC), DeliveryOption.EXACTLY_ONCE) {
            @Override
            public boolean onMessage(Message<String> message) {
                received3.add(message.getPayload());
                return super.onMessage(message);
            }
        };

        Consumer<String> consumer4 = new Consumer<>("test-consumer4", Collections.singleton(TOPIC), DeliveryOption.EXACTLY_ONCE) {
            @Override
            public boolean onMessage(Message<String> message) {
                received4.add(message.getPayload());
                return super.onMessage(message);
            }
        };

        broker.registerConsumer(consumer1);
        broker.registerConsumer(consumer2);
        broker.registerConsumer(consumer3);
        broker.registerConsumer(consumer4);

        broker.publish("test-message", topic, DeliveryOption.EXACTLY_ONCE, 1000, PRODUCER_ID);

        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch endGate = new CountDownLatch(1000);

        try (ExecutorService exec = Executors.newFixedThreadPool(1000)) {

            for (int i = 0; i < 1000; i++) {
                int finalI = i;
                exec.submit(() -> {
                    try {
                        startGate.await();
                        if (finalI % 4 == 1) {
                            broker.registerConsumer(consumer1);
                        } else if (finalI % 4 == 2) {
                            broker.registerConsumer(consumer2);

                        } else {
                            broker.registerConsumer(consumer3);

                        }


                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } finally {
                        endGate.countDown();
                    }
                });
            }

            startGate.countDown();

            try {
                endGate.await();
            } catch (InterruptedException e) {
                System.err.println("Oops something bad happend during awaiting endGate");
            }
            exec.shutdown();
        }


        assertEquals(List.of("test-message"), received1);
        assertEquals(List.of("test-message"), received2);
        assertEquals(List.of("test-message"), received3);
        assertEquals(List.of("test-message"), received4);
    }


    @Test
    void given_multipleConsumers_when_messagePublished_then_allConsumersReceiveMessage() throws InterruptedException {

        String data = "Test message for multiple consumers";
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);

        TestIConsumerWithLatch consumer1 = new TestIConsumerWithLatch("consumer1", TOPIC, latch1);
        TestIConsumerWithLatch consumer2 = new TestIConsumerWithLatch("consumer2", TOPIC, latch2);

        messageBroker.registerConsumer(consumer1);
        messageBroker.registerConsumer(consumer2);


        messageBroker.publish(data, TOPIC, DeliveryOption.BROADCAST, 1000, "test-producer");


        assertTrue(latch1.await(2, TimeUnit.SECONDS));
        assertTrue(latch2.await(2, TimeUnit.SECONDS));
        assertEquals(data, consumer1.getLastMessage().getPayload());
        assertEquals(data, consumer2.getLastMessage().getPayload());
    }

    @Test
    void given_consumerWithMultipleTopics_when_messagesPublished_then_receiveFromAllTopics() throws InterruptedException {

        String otherTopic = "other-topic";
        messageBroker.createTopic(otherTopic);

        CountDownLatch latch = new CountDownLatch(2);
        TestIConsumerWithLatch consumer = new TestIConsumerWithLatch(
                "multi-topic-consumer",
                Set.of(TOPIC, otherTopic),
                latch
        );

        messageBroker.registerConsumer(consumer);


        messageBroker.publish("Message for topic 1", TOPIC, DeliveryOption.BROADCAST, 1000, PRODUCER_ID);
        messageBroker.publish("Message for topic 2", otherTopic, DeliveryOption.BROADCAST, 1000, PRODUCER_ID);


        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertEquals(2, consumer.getReceivedMessages().size());
    }


    @Test
    void given_registeredConsumer_when_messagePublished_then_consumerReceivesMessage() throws InterruptedException {

        String data = "test message for consumer";
        CountDownLatch latch = new CountDownLatch(1);
        TestIConsumerWithLatch consumer = new TestIConsumerWithLatch(CONSUMER_ID, TOPIC, latch);
        messageBroker.registerConsumer(consumer);

        messageBroker.publish(data, TOPIC, DeliveryOption.BROADCAST, 1000, "test-producer");

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertEquals(data, consumer.getLastMessage().getPayload());
    }

    @Test
    void given_registeredConsumer_when_unregistered_then_stopsReceivingMessages() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);
        TestIConsumerWithLatch consumer = new TestIConsumerWithLatch(CONSUMER_ID, TOPIC, latch);
        messageBroker.registerConsumer(consumer);

        messageBroker.publish("First message", TOPIC, DeliveryOption.BROADCAST, 1000, "test-producer");
        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertEquals(1, consumer.getReceivedMessages().size());


        messageBroker.unregisterConsumer(consumer);

        consumer.reset();
        CountDownLatch newLatch = new CountDownLatch(1);
        consumer.setLatch(newLatch);

        messageBroker.publish("Message after unregistration", TOPIC, DeliveryOption.BROADCAST, 1000, "test-producer");

        assertFalse(newLatch.await(1, TimeUnit.SECONDS));
        assertEquals(0, consumer.getReceivedMessages().size());
    }


    private static class TestIConsumer extends Consumer<String> {
        private final Set<String> receivedMessages = ConcurrentHashMap.newKeySet();

        public TestIConsumer(String id, Set<String> topics, DeliveryOption deliveryOption) {
            super(id, topics, deliveryOption);
        }

        @Override
        public boolean onMessage(Message<String> message) {
            receivedMessages.add(message.getPayload());
            return super.onMessage(message);
        }

        public Set<String> getReceivedMessages() {
            return receivedMessages;
        }
    }

    private static class TestIConsumerWithLatch extends Consumer<String> {
        private Message<String> lastMessage;
        private final java.util.List<Message<String>> receivedMessages = Collections.synchronizedList(new java.util.ArrayList<>());
        private CountDownLatch latch;

        public TestIConsumerWithLatch(String consumerId, String topic, CountDownLatch latch) {
            super(consumerId, Collections.singleton(topic), DeliveryOption.AT_LEAST_ONCE);
            this.latch = latch;
        }

        public TestIConsumerWithLatch(String consumerId, Set<String> topics, CountDownLatch latch) {
            super(consumerId, topics, DeliveryOption.AT_LEAST_ONCE);
            this.latch = latch;
        }

        @Override
        public boolean onMessage(Message<String> message) {
            boolean processed = super.onMessage(message);
            if (processed) {
                this.lastMessage = message;
                this.receivedMessages.add(message);
                latch.countDown();
            }
            return processed;
        }

        public Message<String> getLastMessage() {
            return lastMessage;
        }

        public List<Message<String>> getReceivedMessages() {
            return receivedMessages;
        }

        public void reset() {
            this.lastMessage = null;
            this.receivedMessages.clear();
        }

        public void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }
    }



}