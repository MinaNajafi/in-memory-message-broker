package org.example.broker.producer;

import org.example.broker.DeliveryOption;
import org.example.broker.core.Message;
import org.example.broker.core.MessageBroker;

import java.util.UUID;

public class Producer<E> implements IProducer<E> {

    private final MessageBroker<E> messageBroker;
    private final String producerId;
    private final long defaultTtlMillis;
    private final DeliveryOption deliveryOption;

    public Producer(MessageBroker<E> messageBroker, long defaultTtlMillis, DeliveryOption deliveryOption) {
        if (messageBroker == null) {
            throw new IllegalArgumentException("MessageBroker cannot be null");
        }
        if (defaultTtlMillis <= 0) {
            throw new IllegalArgumentException("Default TTL must be positive");
        }

        this.deliveryOption = deliveryOption==null ? DeliveryOption.BROADCAST : deliveryOption;

        this.messageBroker = messageBroker;
        this.producerId = UUID.randomUUID().toString();
        this.defaultTtlMillis = defaultTtlMillis;
    }

    @Override
    public Message<E> send(E payload, String topic) {
        return send(payload, topic, defaultTtlMillis);
    }

    @Override
    public Message<E> send(E payload, String topic, long ttlMillis) {
        return send(payload, topic, ttlMillis, 0);
    }

    @Override
    public Message<E> send(E payload, String topic, long ttlMillis, long timeoutMillis) {
        return messageBroker.publish(payload, topic, deliveryOption, ttlMillis, timeoutMillis, producerId);
    }

    @Override
    public String getProducerId() {
        return producerId;
    }

    @Override
    public DeliveryOption getDeliveryOption() {
        return deliveryOption;
    }
}
