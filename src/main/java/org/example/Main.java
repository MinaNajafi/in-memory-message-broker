package org.example;

import org.example.broker.*;
import org.example.broker.consumer.IConsumer;
import org.example.broker.core.Message;
import org.example.broker.core.MessageBroker;
import org.example.broker.producer.IProducer;
import org.example.broker.producer.Producer;

import java.util.Set;

public class Main {
    public static void main(String[] args) {

        MessageBroker<String> messageBroker = new MessageBroker<>(10, 2, true);
        messageBroker.createTopic("test-topic");

        IProducer<String> producer = new Producer<>(messageBroker, 1000, DeliveryOption.BROADCAST);
        producer.send("hello", "test-topic", 1000, 1000);

        IConsumer<String> consumer = new IConsumer<>() {
            @Override
            public boolean onMessage(Message<String> message) {
                System.out.println("Received message: " + message.getPayload());
                return true;
            }

            @Override
            public String getId() {
                return "";
            }

            @Override
            public Set<String> getSubscribedTopics() {
                return Set.of("test-topic");
            }

            @Override
            public long getLastOffset(String topic) {
                return 0;
            }
        };

        boolean isConsumerRegistered = messageBroker.registerConsumer(consumer);

        if (!isConsumerRegistered) {
            System.err.println("Failed to register consumer for topic: " + consumer.getId());
        }

        try {
            Thread.sleep(500); // simulate time for message processing
        } catch (InterruptedException e) {
            System.err.println("Main thread interrupted");
        }

        boolean isConsumerUnRegistered = messageBroker.unregisterConsumer(consumer);

        if (!isConsumerUnRegistered) {
            System.err.println("Failed to unregister consumer for topic: " + consumer.getId());
        }

        messageBroker.shutdown();

    }

}