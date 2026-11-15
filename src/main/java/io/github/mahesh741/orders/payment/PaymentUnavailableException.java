package io.github.mahesh741.orders.payment;

/** Transient failure: the payment gateway timed out or is down. Retrying is expected to help. */
public class PaymentUnavailableException extends RuntimeException {

    public PaymentUnavailableException(String message) {
        super(message);
    }
}
