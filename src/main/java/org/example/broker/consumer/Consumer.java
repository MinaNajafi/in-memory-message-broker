package org.example.broker.consumer;

import org.example.broker.DeliveryOption;
import org.example.broker.core.Message;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Consumer<E> implements IConsumer<E> {

    private final String id;
    private final Set<String> subscribedTopics;
    private final Map<String, AtomicLong> lastOffsets;
    private final Map<String, Set<UUID>> processedMessageIds;
    private final DeliveryOption deliveryOption;
    private final Map<String, Lock> topicLocks;

    public Consumer(String id,
                    Set<String> topics, DeliveryOption deliveryOption) {

        if (topics == null || topics.isEmpty()) {
            throw new IllegalArgumentException("Topics cannot be null or empty");
        }

        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("Consumer ID cannot be null or empty");
        }

        this.id = id;
        this.subscribedTopics = ConcurrentHashMap.newKeySet();
        this.subscribedTopics.addAll(topics);
        this.lastOffsets = new ConcurrentHashMap<>();
        this.processedMessageIds = new ConcurrentHashMap<>();
        this.deliveryOption = Objects.requireNonNullElse(deliveryOption, DeliveryOption.BROADCAST);
        this.topicLocks = new ConcurrentHashMap<>();

        for (String topic : topics) {
            this.lastOffsets.put(topic, new AtomicLong(-1));
            this.processedMessageIds.put(topic, Collections.newSetFromMap(new ConcurrentHashMap<>()));
            this.topicLocks.put(topic, new ReentrantLock());
        }
    }

    @Override
    public boolean onMessage(Message<E> message) {
        String topic = message.getTopic();

        if (!subscribedTopics.contains(topic)) {
            return false;
        }

        if (deliveryOption == DeliveryOption.EXACTLY_ONCE) {
            UUID messageId = message.getId();
            Set<UUID> processedIds = processedMessageIds.get(topic);
            Lock topicLock = topicLocks.get(topic);

            topicLock.lock();
            try {
                if (processedIds.contains(messageId)) {
                    message.acknowledge();
                    return true;
                }

                processedIds.add(messageId);
            } finally {
                topicLock.unlock();
            }
        }

        try {
            System.out.println("topic : " + topic + " payload " + message.getPayload().toString());
            updateOffset(topic, message.getOffset());
            message.acknowledge();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void updateOffset(String topic, long offset) {
        AtomicLong currentOffset = lastOffsets.get(topic);
        if (currentOffset != null) {
            long existing;
            do {
                existing = currentOffset.get();
                if (offset <= existing) {
                    return;
                }
            } while (!currentOffset.compareAndSet(existing, offset));
        }
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Set<String> getSubscribedTopics() {
        return Collections.unmodifiableSet(subscribedTopics);
    }

    @Override
    public long getLastOffset(String topic) {
        AtomicLong offset = lastOffsets.get(topic);
        return offset != null ? offset.get() : -1;
    }
}
