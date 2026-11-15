package io.github.mahesh741.orders.events;

import java.util.UUID;

/** Published to Kafka when an order is accepted. Keyed by order id so retries keep their order. */
public record OrderCreated(UUID orderId, String customerId, String sku, int quantity, long amountCents) { }
