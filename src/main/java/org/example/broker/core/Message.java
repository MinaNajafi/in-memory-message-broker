package org.example.broker.core;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Message<E> {
    private final E payload;
    private final String topic;
    private final long timestamp;
    private final long expirationTime;
    private final UUID id;
    private final long offset;
    private final String producerId;
    private final AtomicInteger attemptCount = new AtomicInteger(0);
    private volatile boolean acknowledged = false;
    private Message<E> next;
    private final Set<String> acknowledgedConsumers;

    public Message(E payload, String topic, long ttlMillis, long offset, String producerId) {
        this.payload = payload;
        this.topic = topic;
        this.timestamp = System.currentTimeMillis();
        this.expirationTime = timestamp + ttlMillis;
        this.id = UUID.randomUUID();
        this.offset = offset;
        this.producerId = producerId;
        this.next = null;
        this.acknowledgedConsumers = ConcurrentHashMap.newKeySet();
    }

    public boolean isExpired() {
        return System.currentTimeMillis() > expirationTime;
    }


    public void acknowledge() {
        this.acknowledged = true;
    }


    public boolean isAcknowledged() {
        return acknowledged;
    }


    public int incrementAttempt() {
        return attemptCount.incrementAndGet();
    }

    public int getAttemptCount() {
        return attemptCount.get();
    }

    public E getPayload() {
        return payload;
    }

    public String getTopic() {
        return topic;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public UUID getId() {
        return id;
    }

    public long getOffset() {
        return offset;
    }

    public String getProducerId() {
        return producerId;
    }

    Message<E> getNext() {
        return next;
    }

    void setNext(Message<E> next) {
        this.next = next;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message<?> message = (Message<?>) o;
        return id.equals(message.id);
    }


    public void acknowledge(String consumerId) {
        acknowledgedConsumers.add(consumerId);
    }

    public boolean isFullyAcknowledged(Set<String> topicConsumers) {
        if (acknowledgedConsumers == null || acknowledgedConsumers.isEmpty()) {
            return false;
        }
        return acknowledgedConsumers.containsAll(topicConsumers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "Message{" +
                "payload=" + payload +
                ", topic='" + topic + '\'' +
                ", offset=" + offset +
                ", id=" + id +
                '}';
    }
}
