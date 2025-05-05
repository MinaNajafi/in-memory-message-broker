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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class DeadlockDetectionTest {

    private MessageBroker<String> messageBroker;
    private ExecutorService executorService;

    @BeforeEach
    void setUp() {
        messageBroker = new MessageBroker<>(100, 3, false);
        executorService = Executors.newCachedThreadPool();
    }

    @AfterEach
    void tearDown() {
        messageBroker.disableCleanUp();
        executorService.shutdownNow();
        try {
            boolean executorTearDown = executorService.awaitTermination(2, TimeUnit.SECONDS);
            System.out.println("Tear Down Executor service : " + executorTearDown);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void given_cleanupAndPublishing_when_executingConcurrently_then_noDeadlock() throws InterruptedException {

        final String TOPIC = "cleanup-deadlock-topic";
        final int ITERATIONS = 1000;

        MessageBroker<String> brokerWithCleanup = new MessageBroker<>(
                50, 3, true);

        IProducer<String> producer = new Producer<>(brokerWithCleanup, 100, DeliveryOption.BROADCAST);

        IConsumer<String> slowConsumer = new Consumer<>(
                "slow-consumer", Collections.singleton(TOPIC), DeliveryOption.BROADCAST) {
            @Override
            public boolean onMessage(Message<String> message) {
                try {
                    Thread.sleep(60000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return super.onMessage(message);
            }
        };

        brokerWithCleanup.registerConsumer(slowConsumer);

        CountDownLatch completionLatch = new CountDownLatch(1);
        AtomicBoolean deadlockDetected = new AtomicBoolean(false);

        Thread watchdogThread = new Thread(() -> {
            try {
                Thread.sleep(8000);
                if (!completionLatch.await(0, TimeUnit.MILLISECONDS)) {
                    deadlockDetected.set(true);
                    System.err.println("Potential deadlock detected!");

                    System.err.println("Thread dump:");
                    Map<Thread, StackTraceElement[]> traces = Thread.getAllStackTraces();
                    for (Map.Entry<Thread, StackTraceElement[]> entry : traces.entrySet()) {
                        Thread thread = entry.getKey();
                        StackTraceElement[] stackTrace = entry.getValue();

                        System.err.println("Thread: " + thread.getName() + " (State: " + thread.getState() + ")");
                        for (StackTraceElement element : stackTrace) {
                            System.err.println("\tat " + element);
                        }
                        System.err.println();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        watchdogThread.setDaemon(true);
        watchdogThread.start();

        try {
            for (int i = 0; i < ITERATIONS; i++) {
                producer.send("deadlock-test-" + i, TOPIC);

                if (i % 100 == 0) {
                    Thread.sleep(1);
                }
            }
        } finally {
            completionLatch.countDown();
            brokerWithCleanup.disableCleanUp();
        }

        assertFalse(deadlockDetected.get(), "No deadlock should be detected");
    }

    @Test
    void given_multipleOperations_when_executingConcurrently_then_noDeadlock() throws InterruptedException {

        final int THREAD_COUNT = 20;
        final int OPERATIONS_PER_THREAD = 500;
        final String[] TOPICS = {"topic1", "topic2", "topic3", "topic4", "topic5"};

        CountDownLatch operationsComplete = new CountDownLatch(THREAD_COUNT);

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {

                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {

                        int operation = ThreadLocalRandom.current().nextInt(5);
                        String topic = TOPICS[ThreadLocalRandom.current().nextInt(TOPICS.length)];

                        switch (operation) {
                            case 0:
                                messageBroker.createTopic(topic);
                                break;

                            case 1:
                                messageBroker.publish("test-message" + threadId + "-" + j, topic, DeliveryOption.BROADCAST, 5000, "test-producer-" + threadId);
                                break;

                            case 2:
                                IConsumer<String> consumer = new Consumer<>(
                                        "test-consumer-" + threadId + "-" + j,
                                        Collections.singleton(topic),
                                        DeliveryOption.BROADCAST
                                );
                                messageBroker.registerConsumer(consumer);
                                break;

                            case 3:
                                String consumerId = "test-consumer-" + threadId + "-" + (j > 0 ? j - 1 : 0);
                                IConsumer<String> existingConsumer = new Consumer<>(
                                        consumerId,
                                        Collections.singleton(topic),
                                        DeliveryOption.AT_LEAST_ONCE
                                );
                                messageBroker.unregisterConsumer(existingConsumer);
                                break;

                            case 4:
                                messageBroker.getCurrentOffset(topic);
                                break;
                        }

                        if (ThreadLocalRandom.current().nextInt(10) == 0) {
                            Thread.sleep(ThreadLocalRandom.current().nextInt(1, 5));
                        }
                    }
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                } finally {
                    operationsComplete.countDown();
                }
            });
        }

        AtomicBoolean deadlockDetected = new AtomicBoolean(false);

        Thread watchdogThread = new Thread(() -> {
            try {
                Thread.sleep(10000);
                if (operationsComplete.getCount() > 0) {
                    deadlockDetected.set(true);
                    System.err.println("Potential deadlock detected in random operations test!");

                    System.err.println("Thread dump:");
                    Map<Thread, StackTraceElement[]> traces = Thread.getAllStackTraces();
                    for (Map.Entry<Thread, StackTraceElement[]> entry : traces.entrySet()) {
                        Thread thread = entry.getKey();
                        StackTraceElement[] stackTrace = entry.getValue();

                        System.err.println("Thread: " + thread.getName() + " (State: " + thread.getState() + ")");
                        for (StackTraceElement element : stackTrace) {
                            System.err.println("\tat " + element);
                        }
                        System.err.println();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        watchdogThread.setDaemon(true);
        watchdogThread.start();

        boolean completed = operationsComplete.await(12, TimeUnit.SECONDS);

        assertTrue(completed, "All operations should complete without deadlock");
        assertFalse(deadlockDetected.get(), "No deadlock should be detected");
    }

    @Test
    void given_backpressure_when_queueCapacityReached_then_noLivelocks() throws InterruptedException {

        final String TOPIC = "livelock-test-topic";
        final int SMALL_CAPACITY = 10;

        MessageBroker<String> broker = new MessageBroker<>(
                SMALL_CAPACITY, 3, false);

        IProducer<String> producer = new Producer<>(broker, 60000, DeliveryOption.BROADCAST);

        IConsumer<String> slowConsumer = new Consumer<>(
                "slow-livelock-consumer", Collections.singleton(TOPIC), DeliveryOption.BROADCAST) {
            @Override
            public boolean onMessage(Message<String> message) {
                try {

                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return super.onMessage(message);
            }
        };

        broker.registerConsumer(slowConsumer);

        int producerCount = 5;
        CountDownLatch producersComplete = new CountDownLatch(producerCount);
        AtomicInteger totalSuccessfulMessages = new AtomicInteger(0);
        AtomicInteger backpressureEvents = new AtomicInteger(0);

        for (int i = 0; i < producerCount; i++) {
            final int producerId = i;
            executorService.submit(() -> {
                try {
                    for (int j = 0; j < 30; j++) {
                        try {
                            long timeout = 50 + (j * 10);
                            Message<String> message = producer.send(
                                    "livelock-test-" + producerId + "-" + j,
                                    TOPIC, 60000, timeout
                            );

                            if (message != null) {
                                totalSuccessfulMessages.incrementAndGet();
                            }
                        } catch (Exception e) {

                            if (e.getMessage() != null && e.getMessage().contains("Backpressure")) {
                                backpressureEvents.incrementAndGet();
                            } else {
                                System.err.println(e.getMessage());
                            }
                        }

                        Thread.sleep(20);
                    }
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                } finally {
                    producersComplete.countDown();
                }
            });
        }

        boolean completed = producersComplete.await(8, TimeUnit.SECONDS);

        assertTrue(completed, "All producers should complete their operations");
        assertTrue(totalSuccessfulMessages.get() > 0, "Some messages should be successfully sent");

        System.out.println("Total successful messages: " + totalSuccessfulMessages.get());
        System.out.println("Backpressure events: " + backpressureEvents.get());
    }
} 