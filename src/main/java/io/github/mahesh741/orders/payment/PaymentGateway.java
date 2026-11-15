package io.github.mahesh741.orders.payment;

import java.util.UUID;

public interface PaymentGateway {

    /**
     * Capture payment for an order. Idempotent on {@code orderId}: charging the same order
     * twice returns the original reference and does not charge again.
     *
     * @return the payment reference
     * @throws PaymentUnavailableException if the gateway cannot be reached; safe to retry
     */
    String charge(UUID orderId, long amountCents);
}
