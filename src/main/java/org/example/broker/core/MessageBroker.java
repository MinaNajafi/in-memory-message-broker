package org.example.broker.core;

import org.example.broker.DeliveryOption;
import org.example.broker.consumer.IConsumer;
import org.example.broker.exception.BackpressureException;

import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class MessageBroker<E> {

    private static final int DEFAULT_BACKPRESSURE_THRESHOLD = 10000;
    private static final int DEFAULT_MAX_RETRY_ATTEMPTS = 3;
    private static final long DEFAULT_CLEANUP_INTERVAL_MS = 60000;

    private final int backpressureThreshold;
    private final int maxRetryAttempts;
    private final ConcurrentMap<String, Queue<E>> queuePerTopic;
    private final ConcurrentMap<String, Set<IConsumer<E>>> consumersPerTopic;
    private final ScheduledExecutorService cleanupExecutor;
    private final ScheduledExecutorService deliveryExecutor;
    private final ExecutorService processingExecutor;

    public MessageBroker(int backpressureThreshold, int maxRetryAttempts, boolean cleanUpIsNeeded) {

        this.backpressureThreshold = backpressureThreshold == 0 ? DEFAULT_BACKPRESSURE_THRESHOLD : backpressureThreshold;
        this.maxRetryAttempts = maxRetryAttempts == 0 ? DEFAULT_MAX_RETRY_ATTEMPTS : maxRetryAttempts;
        this.queuePerTopic = new ConcurrentHashMap<>();
        this.consumersPerTopic = new ConcurrentHashMap<>();
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor();
        this.deliveryExecutor = Executors.newScheduledThreadPool(2);
        this.processingExecutor = Executors.newCachedThreadPool();

        if (cleanUpIsNeeded) {
            cleanupExecutor.scheduleAtFixedRate(
                    this::cleanupQueues,
                    DEFAULT_CLEANUP_INTERVAL_MS,
                    DEFAULT_CLEANUP_INTERVAL_MS,
                    TimeUnit.MILLISECONDS
            );
        }
    }

    public MessageBroker() {
        this(DEFAULT_BACKPRESSURE_THRESHOLD, DEFAULT_MAX_RETRY_ATTEMPTS, true);
    }

    public void createTopic(String topic) {
        queuePerTopic.putIfAbsent(topic, new Queue<>(topic, backpressureThreshold));
        consumersPerTopic.putIfAbsent(topic, ConcurrentHashMap.newKeySet());
    }

    public Message<E> publish(E payload, String topic, DeliveryOption deliveryOption, long ttlMillis, long timeoutMillis, String producerId) {

        if (payload == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("Topic cannot be null or empty");
        }

        createTopic(topic);

        Queue<E> queue = queuePerTopic.get(topic);
        Message<E> message = null;

        try {
            message = queue.enqueue(payload, producerId, ttlMillis, TimeUnit.MILLISECONDS.toNanos(timeoutMillis));

            if (message != null) {
                final Message<E> finalMessage = message;
                processingExecutor.execute(() -> deliverToConsumers(finalMessage, deliveryOption));
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (BackpressureException e) {
            System.err.println("Backpressure applied for topic: " + topic);
        }

        return message;
    }

    public Message<E> publish(E payload, String topic, DeliveryOption deliveryOption, long ttlMillis, String producerId) {
        return publish(payload, topic, deliveryOption, ttlMillis, 0, producerId);
    }

    public boolean registerConsumer(IConsumer<E> consumer) {

        if (consumer == null) {
            throw new IllegalArgumentException("Consumer cannot be null");
        }

        boolean added = false;

        for (String topic : consumer.getSubscribedTopics()) {
            createTopic(topic);
            Set<IConsumer<E>> consumers = consumersPerTopic.get(topic);
            if (consumers.add(consumer)) {
                added = true;
            }
            deliverMissedMessages(consumer, topic);
        }

        return added;
    }


    public boolean unregisterConsumer(IConsumer<E> consumer) {

        if (consumer == null) {
            throw new IllegalArgumentException("Consumer cannot be null");
        }

        boolean removed = false;

        for (String topic : consumer.getSubscribedTopics()) {
            Set<IConsumer<E>> consumers = consumersPerTopic.get(topic);
            if (consumers != null && consumers.remove(consumer)) {
                removed = true;
            }
        }

        return removed;
    }

    private void deliverToConsumers(Message<E> message, DeliveryOption deliveryOption) {
        String topic = message.getTopic();
        Set<IConsumer<E>> consumers = consumersPerTopic.get(topic);

        if (consumers != null && !consumers.isEmpty()) {
            for (IConsumer<E> consumer : consumers) {
                try {
                    boolean success = consumer.onMessage(message);
                    if (!success) {
                        handleFailure(message, consumer, deliveryOption);
                    } else {
                        message.acknowledge(consumer.getId());
                    }
                } catch (Exception e) {
                    System.err.println("Error delivering message: " + e.getMessage());
                    handleFailure(message, consumer, deliveryOption);
                }
            }
        }
    }

    private void handleFailure(Message<E> message, IConsumer<E> consumer, DeliveryOption deliveryOption) {

        if (deliveryOption == DeliveryOption.NO_GUARANTEES) {
            return;
        }

        int attempts = message.incrementAttempt();

        if (attempts <= maxRetryAttempts || deliveryOption == DeliveryOption.AT_LEAST_ONCE) {
            long delay = Math.min(
                    (long) Math.pow(2, Math.min(attempts - 1, 10)),
                    60000
            );
            if (attempts > maxRetryAttempts) {
                delay = 60000;
            }
            scheduleRetryDelivery(message, consumer, delay, deliveryOption);
        }
    }

    private void scheduleRetryDelivery(Message<E> message, IConsumer<E> consumer, long delay, DeliveryOption option) {

        deliveryExecutor.schedule(() -> {
            if (!message.isExpired()) {
                try {
                    boolean success = consumer.onMessage(message);
                    System.out.println("success delivery of message: " + message.getPayload());
                    if (!success) {
                        handleFailure(message, consumer, option);
                    }
                    System.out.println("success delivery of message: " + message.getPayload());
                } catch (Exception e) {
                    System.err.println("Error in retry delivery: " + e.getMessage());
                    if (option == DeliveryOption.AT_LEAST_ONCE) {
                        handleFailure(message, consumer, option);
                    }
                }
            }
        }, delay, TimeUnit.MILLISECONDS);

    }

    private void deliverMissedMessages(IConsumer<E> consumer, String topic) {

        Queue<E> queue = queuePerTopic.get(topic);
        if (queue == null) return;

        long lastOffset = consumer.getLastOffset(topic);
        List<Message<E>> missedMessages = queue.getMessagesAfterOffset(lastOffset);

        for (Message<E> message : missedMessages) {
            final Message<E> finalMessage = message;
            final IConsumer<E> finalConsumer = consumer;

            processingExecutor.execute(() -> {
                try {
                    boolean success = finalConsumer.onMessage(finalMessage);
                    if (!success) {
                        handleFailure(finalMessage, finalConsumer, DeliveryOption.BROADCAST);
                    }
                } catch (Exception e) {
                    System.err.println("Error delivering missed message: " + e.getMessage());
                    handleFailure(finalMessage, finalConsumer, DeliveryOption.BROADCAST);
                }
            });
        }

    }

    private void cleanupQueues() {
        for (Queue<E> queue : queuePerTopic.values()) {
            try {
                Set<String> ids = consumersPerTopic.get(queue.getTopic()).stream()
                        .map(IConsumer::getId)
                        .collect(Collectors.toSet());
                int removed = queue.cleanup(ids);
                if (removed > 0) {
                    System.out.println("Cleaned up " + removed + " messages from topic: " + queue.getTopic() + " now size is " + consumersPerTopic.get(queue.getTopic()).size());
                }
            } catch (Exception e) {
                System.err.println("Error during queue cleanup: " + e.getMessage());
            }
        }
    }

    public long getCurrentOffset(String topic) {
        Queue<E> queue = queuePerTopic.get(topic);
        return queue != null ? queue.getCurrentOffset() : -1;
    }

    public void disableCleanUp() {
        cleanupExecutor.shutdownNow();
    }

    public int getRegisteredConsumersCount(String topic) {
        return consumersPerTopic.get(topic).size();
    }

    public void shutdown() {

        disableCleanUp();

        shutdownExecutorService(processingExecutor, "Processing Executor");
        shutdownExecutorService(deliveryExecutor, "Delivery Executor");
        shutdownExecutorService(cleanupExecutor, "Cleanup Executor");

    }

    private void shutdownExecutorService(ExecutorService executor, String executorName) {
        if (executor == null || executor.isShutdown()) {
            return;
        }

        try {

            executor.shutdown();

            boolean terminated = executor.awaitTermination(5, TimeUnit.SECONDS);

            if (!terminated) {

                System.out.println("Forcing shutdown of " + executorName + " after timeout");
                executor.shutdownNow();

                terminated = executor.awaitTermination(2, TimeUnit.SECONDS);

                if (!terminated) {
                    System.err.println("Warning: " + executorName + " did not terminate completely");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println(executorName + " shutdown interrupted, forcing immediate shutdown");
            executor.shutdownNow();
        }
    }


}