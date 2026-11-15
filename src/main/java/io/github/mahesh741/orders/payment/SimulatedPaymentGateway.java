package io.github.mahesh741.orders.payment;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

/**
 * Stand-in for a real payment provider. An outage can be switched on to demonstrate the
 * retry and dead-letter path, either from the API or from a test.
 */
@Component
public class SimulatedPaymentGateway implements PaymentGateway {

    private final Map<UUID, String> charges = new ConcurrentHashMap<>();
    private final AtomicBoolean outage = new AtomicBoolean();
    private final AtomicInteger failuresRemaining = new AtomicInteger();
    private final AtomicInteger calls = new AtomicInteger();

    @Override
    public String charge(UUID orderId, long amountCents) {
        calls.incrementAndGet();
        String existing = charges.get(orderId);
        if (existing != null) {
            return existing;
        }
        if (outage.get()) {
            throw new PaymentUnavailableException("payment gateway is unavailable");
        }
        if (failuresRemaining.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            throw new PaymentUnavailableException("payment gateway timed out");
        }
        return charges.computeIfAbsent(orderId, id -> "pay_" + id.toString().substring(0, 8));
    }

    public void setOutage(boolean enabled) {
        outage.set(enabled);
    }

    public boolean isOutage() {
        return outage.get();
    }

    /** Make the next {@code n} charge attempts fail, then recover. */
    public void failNext(int n) {
        failuresRemaining.set(n);
    }

    public int chargeCount() {
        return charges.size();
    }

    public int callCount() {
        return calls.get();
    }

    public boolean hasCharged(UUID orderId) {
        return charges.containsKey(orderId);
    }
}
