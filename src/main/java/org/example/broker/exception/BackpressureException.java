package org.example.broker.exception;

public class BackpressureException extends RuntimeException {
    public BackpressureException(String message) {
        super(message);
    }
}