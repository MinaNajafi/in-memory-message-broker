package org.example.broker.consumer;

import org.example.broker.core.Message;

import java.util.Set;

public interface IConsumer<E> {

    boolean onMessage(Message<E> message);

    String getId();

    Set<String> getSubscribedTopics();

    long getLastOffset(String topic);
}
