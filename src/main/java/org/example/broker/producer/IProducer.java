package org.example.broker.producer;

import org.example.broker.core.Message;
import org.example.broker.DeliveryOption;

public interface IProducer<E> {

    Message<E> send(E payload, String topic);

    Message<E> send(E payload, String topic, long ttlMillis);

    Message<E> send(E payload, String topic, long ttlMillis, long timeoutMillis);

    String getProducerId();

    DeliveryOption getDeliveryOption();
}
