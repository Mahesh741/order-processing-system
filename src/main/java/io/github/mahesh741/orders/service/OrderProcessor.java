package io.github.mahesh741.orders.service;

import io.github.mahesh741.orders.domain.DeadLetter;
import io.github.mahesh741.orders.domain.DeadLetterRepository;
import io.github.mahesh741.orders.domain.Order;
import io.github.mahesh741.orders.domain.OrderRepository;
import io.github.mahesh741.orders.domain.OrderStatus;
import io.github.mahesh741.orders.events.OrderCreated;
import io.github.mahesh741.orders.payment.PaymentGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/**
 * Consumes order events.
 *
 * <p>Failures are retried on separate retry topics with exponential backoff, so one slow or
 * failing order never blocks the partition behind it. When the attempts are used up, or the
 * failure is one retrying cannot fix, the event lands on the dead-letter topic.
 *
 * <pre>
 * orders.created -> orders.created-retry-0 -> -retry-1 -> -retry-2 -> orders.created-dlt
 * </pre>
 */
@Component
public class OrderProcessor {

    private static final Logger log = LoggerFactory.getLogger(OrderProcessor.class);

    private final OrderRepository orders;
    private final DeadLetterRepository deadLetters;
    private final InventoryService inventory;
    private final PaymentGateway payments;

    public OrderProcessor(OrderRepository orders, DeadLetterRepository deadLetters,
                          InventoryService inventory, PaymentGateway payments) {
        this.orders = orders;
        this.deadLetters = deadLetters;
        this.inventory = inventory;
        this.payments = payments;
    }

    @RetryableTopic(
            attempts = "${app.retry.attempts}",
            backoff = @Backoff(delayExpression = "${app.retry.delay-ms}", multiplierExpression = "${app.retry.multiplier}"),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            exclude = InvalidOrderException.class,
            autoCreateTopics = "true",
            numPartitions = "${app.topics.partitions}",
            replicationFactor = "1")
    @KafkaListener(topics = "${app.topics.orders}")
    public void process(OrderCreated event) {
        Order order = orders.findById(event.orderId())
                .orElseThrow(() -> new InvalidOrderException("order " + event.orderId() + " does not exist"));

        // Kafka delivers at least once, so the same event can arrive again after a rebalance.
        // Only a PENDING order is processed: a completed one must not be charged again, and a
        // failed one waits for an explicit replay, which sets it back to PENDING first.
        if (order.getStatus() != OrderStatus.PENDING) {
            log.info("order {} is {}, ignoring duplicate event", order.getId(), order.getStatus());
            return;
        }

        order.recordAttempt();
        orders.save(order);
        try {
            inventory.reserve(order.getSku(), order.getQuantity());
            String reference = payments.charge(order.getId(), order.getAmountCents());
            order.complete(reference);
            orders.save(order);
            log.info("order {} completed on attempt {}", order.getId(), order.getAttempts());
        } catch (RuntimeException e) {
            order.recordFailure(e.getMessage());
            orders.save(order);
            log.warn("order {} failed on attempt {}: {}", order.getId(), order.getAttempts(), e.getMessage());
            throw e;
        }
    }

    @DltHandler
    public void onDeadLetter(OrderCreated event, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        orders.findById(event.orderId()).ifPresentOrElse(order -> {
            order.fail();
            orders.save(order);
            String reason = order.getFailureReason() == null ? "unknown" : order.getFailureReason();
            deadLetters.save(new DeadLetter(order.getId(), topic, reason));
            log.error("order {} dead-lettered after {} attempts: {}", order.getId(), order.getAttempts(), reason);
        }, () -> log.error("dead letter for unknown order {}", event.orderId()));
    }
}
