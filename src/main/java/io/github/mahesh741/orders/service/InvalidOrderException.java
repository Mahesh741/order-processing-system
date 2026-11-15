package io.github.mahesh741.orders.service;

/** Permanent failure: retrying cannot fix it, so the event goes straight to the dead-letter topic. */
public class InvalidOrderException extends RuntimeException {

    public InvalidOrderException(String message) {
        super(message);
    }
}
