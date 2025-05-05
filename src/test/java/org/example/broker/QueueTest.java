package org.example.broker;

import static org.junit.jupiter.api.Assertions.*;

import org.example.broker.core.Message;
import org.example.broker.core.Queue;
import org.example.broker.exception.BackpressureException;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.*;

public class QueueTest {

    private final String TOPIC = "test-topic";
    private static final String PRODUCER_ID = "test-producer";

    @Test
    void givenNullPayload_whenEnqueue_thenThrowsNullPointer() {
        Queue<String> queue = new Queue<>(TOPIC, 1);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> queue.enqueue(null, PRODUCER_ID, 1000, 0));
        assertEquals("Payload cannot be null", exception.getMessage());
    }

    @Test
    void givenFullQueueWithoutTimeout_whenEnqueue_thenThrowsBackpressure() throws InterruptedException {
        Queue<String> queue = new Queue<>(TOPIC, 1);
        queue.enqueue("test-message1", PRODUCER_ID, 1000, 0);
        assertThrows(BackpressureException.class, () -> queue.enqueue("test-message2", PRODUCER_ID, 1000, 0));
    }

    @Test
    void givenFullQueueWithTimeout_whenEnqueue_thenWaitsThenThrowsBackpressure() throws InterruptedException {
        Queue<String> queue = new Queue<>(TOPIC, 1);
        queue.enqueue("test-message1", PRODUCER_ID, 1000, 0);
        long start = System.nanoTime();
        assertThrows(BackpressureException.class, () -> queue.enqueue("test-message2", PRODUCER_ID, 1000, TimeUnit.MILLISECONDS.toNanos(500)));
        long waitedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(waitedMs >= 500, "Should wait at least the timeout before throwing");
    }

    @Test
    void givenEmptyQueueAndDelayedProducer_whenPoll_thenReturnsMessage() throws InterruptedException {
        Queue<String> queue = new Queue<>(TOPIC, 1);
        try (ExecutorService exec = Executors.newSingleThreadExecutor()) {
            exec.submit(() -> {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                try {
                    queue.enqueue("test-message", PRODUCER_ID, 1000, 0);
                } catch (Exception ignored) {
                }
            });

            Message<String> msg = queue.poll(TimeUnit.MILLISECONDS.toNanos(500));
            assertNotNull(msg);
            assertEquals("test-message", msg.getPayload());
            exec.shutdown();
        }
    }

    @Test
    void givenExpiredAndAcknowledgedMessages_whenCleanup_thenRemovesThem() throws InterruptedException {
        Queue<String> queue = new Queue<>(TOPIC, 2);
        Message<String> m1 = queue.enqueue("test-message1", PRODUCER_ID, 1, 0);
        Thread.sleep(2);

        Message<String> m2 = queue.enqueue("test-message2", PRODUCER_ID, 1000, 0);
        m2.acknowledge("test-consumer");

        int removed = queue.cleanup(Collections.singleton("test-consumer"));
        assertEquals(2, removed);
        assertNull(queue.peek());
    }


}
