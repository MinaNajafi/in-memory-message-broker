package org.example.broker;

public enum DeliveryOption {
    EXACTLY_ONCE,
    AT_LEAST_ONCE,
    NO_GUARANTEES,
    BROADCAST
}
