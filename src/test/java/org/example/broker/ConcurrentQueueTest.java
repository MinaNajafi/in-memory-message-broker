package org.example.broker;

import org.example.broker.core.Message;
import org.example.broker.core.Queue;
import org.example.broker.exception.BackpressureException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class ConcurrentQueueTest {

    private static final int QUEUE_CAPACITY = 100;
    private Queue<String> queue;
    private final String TOPIC = "test-topic";

    @BeforeEach
    void setUp() {
        queue = new Queue<>(TOPIC, QUEUE_CAPACITY);
    }

    @Test
    void given_multipleProducers_when_enqueueSimultaneously_then_allMessagesStored() throws InterruptedException {

        int numThreads = 10;
        int messagesPerThread = 50;
        AtomicInteger successCount;
        try (ExecutorService executorService = Executors.newFixedThreadPool(numThreads)) {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completionLatch = new CountDownLatch(numThreads);
            successCount = new AtomicInteger(0);

            for (int i = 0; i < numThreads; i++) {
                final int threadId = i;
                executorService.submit(() -> {
                    try {
                        startLatch.await();
                        for (int j = 0; j < messagesPerThread; j++) {
                            String payload = "message-" + threadId + "-" + j;
                            Message<String> message = queue.enqueue(payload, "producer-" + threadId, 60000, 0);
                            if (message != null) {
                                successCount.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Error in thread " + threadId + ": " + e.getMessage());
                    } finally {
                        completionLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            completionLatch.await();
            executorService.shutdown();
        }


        assertEquals(numThreads * messagesPerThread, successCount.get(), "All messages should be successfully enqueued");
        assertEquals(numThreads * messagesPerThread, queue.size(), "Queue size should match the total number of messages");
    }

    @Test
    void given_multipleConsumers_when_dequeueSimultaneously_then_eachMessageProcessedOnce() throws InterruptedException {

        int messageCount = 1000;
        for (int i = 0; i < messageCount; i++) {
            queue.enqueue("message-" + i, "producer", 60000, 0);
        }

        int numThreads = 10;
        ConcurrentHashMap<String, Boolean> processedMessages;
        try (ExecutorService executorService = Executors.newFixedThreadPool(numThreads)) {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completionLatch = new CountDownLatch(numThreads);

            processedMessages = new ConcurrentHashMap<>();


            for (int i = 0; i < numThreads; i++) {
                executorService.submit(() -> {
                    try {
                        startLatch.await();
                        while (true) {
                            try {
                                Message<String> message = queue.poll(100);
                                if (message == null) {
                                    break;
                                }

                                String payload = message.getPayload();
                                if (processedMessages.putIfAbsent(payload, Boolean.TRUE) != null) {
                                    fail("Message " + payload + " was processed more than once");
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Error in consumer thread: " + e.getMessage());
                    } finally {
                        completionLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            completionLatch.await();
            executorService.shutdown();
        }

        assertEquals(messageCount, processedMessages.size(), "All messages should be processed exactly once");
        assertEquals(0, queue.size(), "Queue should be empty after all messages are processed");
    }

    @Test
    void given_concurrentProducersAndConsumers_when_operatingSimultaneously_then_allMessagesProcessedWithoutLoss() throws InterruptedException {

        int producerThreads = 5;
        int consumerThreads = 5;
        int messagesPerProducer = 200;

        ExecutorService consumerExecutor;
        ConcurrentHashMap<String, Boolean> producedMessages;
        ConcurrentHashMap<String, Boolean> consumedMessages;
        try (ExecutorService producerExecutor = Executors.newFixedThreadPool(producerThreads)) {
            consumerExecutor = Executors.newFixedThreadPool(consumerThreads);

            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch producersComplete = new CountDownLatch(producerThreads);

            producedMessages = new ConcurrentHashMap<>();
            consumedMessages = new ConcurrentHashMap<>();


            for (int i = 0; i < producerThreads; i++) {
                final int producerId = i;
                producerExecutor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int j = 0; j < messagesPerProducer; j++) {
                            String payload = "producer-" + producerId + "-message-" + j;
                            Message<String> message = queue.enqueue(payload, "producer-" + producerId, 60000, 1000);
                            producedMessages.put(payload, message != null);
                        }
                    } catch (Exception e) {
                        System.err.println("Error in producer thread: " + e.getMessage());
                    } finally {
                        producersComplete.countDown();
                    }
                });
            }


            for (int i = 0; i < consumerThreads; i++) {
                consumerExecutor.submit(() -> {
                    try {
                        startLatch.await();
                        while (!Thread.currentThread().isInterrupted()) {
                            try {
                                Message<String> message = queue.poll(100);
                                if (message != null) {
                                    consumedMessages.put(message.getPayload(), Boolean.TRUE);
                                } else if (producersComplete.getCount() == 0 && queue.size() == 0) {
                                    break;
                                }
                                Thread.sleep(1);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Error in consumer thread: " + e.getMessage());
                    }
                });
            }

            startLatch.countDown();

            producersComplete.await();

            Thread.sleep(2000);

            producerExecutor.shutdown();
            consumerExecutor.shutdown();

            producerExecutor.awaitTermination(5, TimeUnit.SECONDS);
        }
        consumerExecutor.shutdownNow();
        consumerExecutor.awaitTermination(5, TimeUnit.SECONDS);

        assertEquals(producerThreads * messagesPerProducer, producedMessages.size(), "All expected messages should be produced");
        assertEquals(producedMessages.size(), consumedMessages.size(), "All produced messages should be consumed");

        for (String message : producedMessages.keySet()) {
            assertTrue(consumedMessages.containsKey(message), "Message " + message + " was produced but not consumed");
        }

    }

    @Test
    void given_queueAtCapacity_when_enqueueAttempted_then_backpressureApplied() {

        Queue<String> smallQueue = new Queue<>(TOPIC, 10);

        for (int i = 0; i < 10; i++) {
            try {
                smallQueue.enqueue("message-" + i, "producer", 60000, 0);
            } catch (Exception e) {
                fail("Should not throw exception during initial fill: " + e.getMessage());
            }
        }

        assertThrows(BackpressureException.class, () ->
                        smallQueue.enqueue("overflow-message", "producer", 60000, 0),
                "Should throw BackpressureException when queue is full and timeout is 0"
        );

        assertEquals(10, smallQueue.size(), "Queue size should still be at capacity");
    }

    @Test
    void given_highContention_when_concurrentOperations_then_maintainsConsistency() throws InterruptedException {

        int threadCount = 20;
        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completionLatch = new CountDownLatch(threadCount);

            for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        if (threadId % 2 == 0) {

                            for (int j = 0; j < 50; j++) {
                                try {
                                    queue.enqueue("thread-" + threadId + "-msg-" + j, "producer-" + threadId, 60000, 500);
                                    Thread.sleep(ThreadLocalRandom.current().nextInt(1, 5));
                                } catch (BackpressureException e) {
                                    j--;
                                }
                            }
                        } else {

                            for (int j = 0; j < 50; j++) {
                                Message<String> message = queue.poll(500);
                                if (message != null) {
                                    Thread.sleep(ThreadLocalRandom.current().nextInt(1, 5));
                                } else {
                                    j--;
                                }
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Error in concurrent operations: " + e.getMessage());
                    } finally {
                        completionLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            completionLatch.await();
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }

        int finalSize = queue.size();
        assertTrue(finalSize >= 0 && finalSize <= QUEUE_CAPACITY,
                "Queue size should be non-negative and not exceed capacity");
    }

    @Test
    void givenConcurrentProducersAndConsumers_whenAllFinish_thenQueueSizeIsZero() throws InterruptedException {
        Queue<Integer> queue = new Queue<>(TOPIC, 1000);
        int threads = 10, perThread = 100;
        try (ExecutorService exec = Executors.newFixedThreadPool(threads * 2)) {
            CountDownLatch latch = new CountDownLatch(threads * 2);

            for (int i = 0; i < threads; i++) {
                exec.submit(() -> {
                    for (int j = 0; j < perThread; j++) {
                        try {
                            queue.enqueue(j, "test-producer", 1000, 0);
                        } catch (Exception ignored) {
                        }
                    }
                    latch.countDown();
                });
            }

            for (int i = 0; i < threads; i++) {
                exec.submit(() -> {
                    for (int j = 0; j < perThread; j++) {
                        try {
                            queue.poll(TimeUnit.SECONDS.toNanos(1));
                        } catch (Exception ignored) {
                        }
                    }
                    latch.countDown();
                });
            }

            latch.await();
            assertEquals(0, queue.size());
            exec.shutdown();
        }
    }

    @Test
    void givenConcurrentProducersAndConsumers_whenAllStartTogether_thenQueueSizeIsZero() throws InterruptedException {
        Queue<Integer> queue = new Queue<>(TOPIC, 1000);
        int threads = 10, perThread = 100;
        try (ExecutorService exec = Executors.newFixedThreadPool(threads * 2)) {

            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch finishLatch = new CountDownLatch(threads * 2);

            for (int i = 0; i < threads; i++) {
                exec.submit(() -> {
                    try {
                        startLatch.await();
                        for (int j = 0; j < perThread; j++) {
                            try {
                                queue.enqueue(j, "test-producer", 1000, 0);
                            } catch (Exception ignored) {
                            }
                        }
                    } catch (InterruptedException ignored) {
                    } finally {
                        finishLatch.countDown();
                    }
                });
            }

            for (int i = 0; i < threads; i++) {
                exec.submit(() -> {
                    try {
                        startLatch.await();
                        for (int j = 0; j < perThread; j++) {
                            try {
                                queue.poll(TimeUnit.SECONDS.toNanos(1));
                            } catch (Exception ignored) {
                            }
                        }
                    } catch (InterruptedException ignored) {
                    } finally {
                        finishLatch.countDown();
                    }
                });
            }

            startLatch.countDown();

            finishLatch.await();
            exec.shutdown();
        }

        assertEquals(0, queue.size());
    }


} 