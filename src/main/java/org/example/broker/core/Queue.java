package org.example.broker.core;

import org.example.broker.exception.BackpressureException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Queue<E> {

    private final Message<E> head;
    private Message<E> tail;

    private final int capacity;
    private final AtomicInteger count;
    private final AtomicLong currentOffset;
    private final ReentrantLock lock;
    private final Condition notEmpty;
    private final Condition notFull;
    private final String topic;

    public Queue(String topic, int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be greater than zero");
        }
        this.topic = topic;
        this.capacity = capacity;
        this.head = this.tail = new Message<>(null, topic, 0, -1, "");
        currentOffset = new AtomicLong(0);
        lock = new ReentrantLock();
        notFull = lock.newCondition();
        notEmpty = lock.newCondition();
        count = new AtomicInteger(0);
    }

    public Message<E> enqueue(E payload, String producerId, long ttlMillis, long timeout)
            throws InterruptedException, BackpressureException {
        if (payload == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }

        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            while (count.get() >= capacity) {
                if (timeout <= 0) {
                    throw new BackpressureException("Queue capacity exceeded for topic: " + topic);
                }
                timeout = notFull.awaitNanos(timeout);
            }

            Message<E> message = new Message<>(payload, topic, ttlMillis,
                    currentOffset.getAndIncrement(), producerId);

            tail.setNext(message);
            tail = message;
            count.incrementAndGet();

            notEmpty.signal();
            return message;
        } finally {
            lock.unlock();
        }
    }


    public Message<E> peek() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return peek(false);
        } finally {
            lock.unlock();
        }
    }

    private Message<E> peek(boolean cleanupExpired) {
        Message<E> first = head.getNext();
        if (first == null) {
            return null;
        }

        if (cleanupExpired && first.isExpired()) {
            Message<E> dequeued = dequeue();
            if (dequeued != null) {
                System.out.println("Dequeued expired message: " + dequeued.getPayload());
            }
            return peek(true);
        }

        return first;
    }

    public Message<E> poll(long timeout) throws InterruptedException {
        long nanos = timeout;
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            Message<E> first;
            while ((first = peek(true)) == null) {
                if (nanos <= 0) {
                    return null;
                }
                nanos = notEmpty.awaitNanos(nanos);
            }

            dequeue();
            return first;
        } finally {
            lock.unlock();
        }
    }

    public List<Message<E>> getMessagesAfterOffset(long fromOffset) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            List<Message<E>> result = new ArrayList<>();
            Message<E> current = head.getNext();
            result.add(current);
            return result;

        } finally {
            lock.unlock();
        }
    }


    public int cleanup(Set<String> topicConsumerIds) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            int removed = 0;
            Message<E> pred = head;
            Message<E> curr = head.getNext();
            while (curr != null) {
                if (curr.isExpired() || curr.isFullyAcknowledged(topicConsumerIds)) {
                    pred.setNext(curr.getNext());
                    if (curr == tail) {
                        tail = pred;
                    }
                    count.decrementAndGet();
                    removed++;
                    curr = pred.getNext();
                } else {
                    pred = curr;
                    curr = curr.getNext();
                }
            }

            if (removed > 0) {
                notFull.signalAll();
            }

            return removed;
        } finally {
            lock.unlock();
        }
    }

    private Message<E> dequeue() {
        Message<E> first = head.getNext();
        if (first == null) {
            return null;
        }

        head.setNext(first.getNext());
        if (first.getNext() == null) {
            tail = head;
        }
        first.setNext(null);
        count.decrementAndGet();
        notFull.signal();
        return first;
    }

    public int size() {
        return count.get();
    }

    public int capacity() {
        return capacity;
    }

    public String getTopic() {
        return topic;
    }

    public long getCurrentOffset() {
        return currentOffset.get() - 1;
    }
}
