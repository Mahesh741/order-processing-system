package io.github.mahesh741.orders.domain;

public enum OrderStatus {
    /** Accepted and waiting for the processor. */
    PENDING,
    /** Inventory reserved and payment captured. */
    COMPLETED,
    /** Retries exhausted or the order was invalid; a dead letter was recorded. */
    FAILED
}
